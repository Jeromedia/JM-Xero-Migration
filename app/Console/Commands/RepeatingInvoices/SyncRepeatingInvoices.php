<?php

namespace App\Console\Commands\RepeatingInvoices;

use App\Models\XeroOrganisation;
use App\Services\Xero\XeroIdMapper;
use Carbon\Carbon;
use Illuminate\Console\Command;
use Illuminate\Http\Client\Response;
use Illuminate\Support\Collection;
use Illuminate\Support\Facades\Http;
use Illuminate\Support\Facades\Log;

class SyncRepeatingInvoices extends Command
{
    protected $signature = 'xero:repeating-invoices:sync
                        {--live : Use target instead of test}
                        {--name= : Sync only one repeating invoice by exact Contact Name}
                        {--source-id= : Sync only one repeating invoice by source RepeatingInvoiceID}
                        {--next-date= : Sync only invoices whose source next scheduled date is this YYYY-MM-DD}
                        {--first : Sync only the first filtered repeating invoice}
                        {--all-non-deleted : Include non-deleted ACCREC templates even if not future scheduled}
                        {--limit= : Limit number of ACCREC templates processed after filtering}
                        {--batch= : Process the next N filtered invoices that are not already validly migrated}
                        {--dry-run : Do not write anything, only analyse and count}
                        {--allow-missing-branding : Allow missing branding theme mapping and omit branding on create}';

    protected $description = 'Sync ACCREC repeating invoice templates from SOURCE to TEST or TARGET with strict dependency validation and hard-stop error handling';

    private const ENTITY = 'repeating_invoice';
    private const TAKEOVER_DATE = '2026-03-23';

    public function handle(): int
    {
        $destinationRole = $this->option('live') ? 'target' : 'test';
        $dryRun = (bool) $this->option('dry-run');

        $this->info('Starting repeating invoices sync (STRICT SAFE MODE)...');
        $this->info('Source role: source');
        $this->info('Destination role: ' . $destinationRole);
        $this->info('Mode: ' . ($dryRun ? 'DRY RUN' : 'LIVE WRITE'));
        $this->info('Type mode: ACCREC only');
        $this->info('Takeover date: ' . self::TAKEOVER_DATE);

        if ($this->option('name')) {
            $this->info('Filter mode: exact contact name = ' . $this->option('name'));
        }

        if ($this->option('source-id')) {
            $this->info('Filter mode: source RepeatingInvoiceID = ' . $this->option('source-id'));
        }

        if ($this->option('next-date')) {
            $this->info('Filter mode: source next scheduled date = ' . $this->option('next-date'));
        }

        if ($this->option('first')) {
            $this->info('Filter mode: first source repeating invoice only');
        }

        if ($this->option('batch')) {
            $this->info('Batch mode: next remaining filtered repeating invoices = ' . $this->option('batch'));
        }

        if ($this->option('all-non-deleted')) {
            $this->info('Selection mode: all non-deleted ACCREC templates');
        } else {
            $this->info('Selection mode: future scheduled non-deleted ACCREC templates only');
        }

        if ($this->option('allow-missing-branding')) {
            $this->warn('Branding mode: missing branding mappings will be omitted from payload');
        } else {
            $this->info('Branding mode: missing branding mapping will fail');
        }

        $mapper = new XeroIdMapper();

        $source = XeroOrganisation::where('role', 'source')->first();
        $destination = XeroOrganisation::where('role', $destinationRole)->first();

        if (!$source || !$destination) {
            $this->error('Source or destination organisation not found.');
            return Command::FAILURE;
        }

        if (!$source->token || !$destination->token) {
            $this->error('Source or destination token not found.');
            return Command::FAILURE;
        }

        $sourceTenantId = $this->getTenantId($source);
        $destinationTenantId = $this->getTenantId($destination);

        if (!$sourceTenantId || !$destinationTenantId) {
            $this->error('Source or destination tenant ID not found.');
            return Command::FAILURE;
        }

        $this->info('Loading destination dependency indexes...');

        $destinationAccounts = $this->fetchAccounts($destination, strtoupper($destinationRole));
        $destinationAccountsByCode = $destinationAccounts
            ->filter(fn($account) => !empty($account['Code']))
            ->keyBy(fn($account) => trim((string) $account['Code']));

        $destinationTaxRates = $this->fetchTaxRates($destination, strtoupper($destinationRole));
        $destinationTaxTypes = $this->buildDestinationTaxTypeSet($destinationTaxRates);

        $destinationItems = $this->fetchItems($destination, strtoupper($destinationRole));
        $destinationItemsByCode = $destinationItems
            ->filter(fn($item) => !empty($item['Code']))
            ->keyBy(fn($item) => trim((string) $item['Code']));

        $destinationTrackingCategories = $this->fetchTrackingCategories($destination, strtoupper($destinationRole));
        [$destinationTrackingByCategoryId, $destinationTrackingByOptionId] = $this->buildDestinationTrackingIndexes($destinationTrackingCategories);

        $destinationRepeatingInvoices = $this->fetchRepeatingInvoices($destination, strtoupper($destinationRole));
        [$destinationRepeatingById, $destinationRepeatingByFingerprint] = $this->buildDestinationRepeatingIndexes(
            $destinationRepeatingInvoices,
            $destinationTrackingByOptionId
        );

        $filteredDestinationRepeatingInvoices = $this->filterDestinationRepeatingInvoices($destinationRepeatingInvoices);

        $this->info('Fetching SOURCE repeating invoices...');
        $allSourceRepeatingInvoices = $this->fetchRepeatingInvoices($source, 'SOURCE');

        $skippedAccPay = $allSourceRepeatingInvoices->filter(function (array $invoice) {
            return strtoupper(trim((string) ($invoice['Type'] ?? ''))) === 'ACCPAY';
        })->count();

        $sourceRepeatingInvoices = $this->filterSourceRepeatingInvoices($allSourceRepeatingInvoices);

        if ($sourceRepeatingInvoices->isEmpty()) {
            $this->warn('No ACCREC repeating invoices matched the selected filters.');
            $this->line("Filtered target ACCREC: {$filteredDestinationRepeatingInvoices->count()}");
            $this->line("Skipped ACCPAY templates: {$skippedAccPay}");
            return Command::SUCCESS;
        }

        $fullFilteredSourceCount = $sourceRepeatingInvoices->count();
        $filteredTargetCount = $filteredDestinationRepeatingInvoices->count();

        if ($this->option('first')) {
            $sourceRepeatingInvoices = collect([$sourceRepeatingInvoices->first()]);
        }

        $limit = (int) ($this->option('limit') ?? 0);
        $batch = (int) ($this->option('batch') ?? 0);

        if ($limit > 0 && $batch > 0) {
            $this->warn('Both --limit and --batch were provided. Batch mode will be used and limit will be ignored.');
        }

        if ($batch > 0) {
            $sourceRepeatingInvoices = $this->selectBatchInvoices(
                $sourceRepeatingInvoices,
                $batch,
                $destinationRole,
                $mapper,
                $source,
                $destination,
                $destinationAccountsByCode,
                $destinationTaxTypes,
                $destinationItemsByCode,
                $destinationTrackingByCategoryId,
                $destinationTrackingByOptionId,
                $destinationRepeatingById
            );

            $this->info("Batch mode: processing up to {$batch} remaining repeating invoices");
        } elseif ($limit > 0) {
            $sourceRepeatingInvoices = $sourceRepeatingInvoices->take($limit)->values();
            $this->info("Limit mode: processing up to {$limit} repeating invoices");
        }

        $selectedSourceCount = $sourceRepeatingInvoices->count();

        $processed = 0;
        $mappedValid = 0;
        $mappedInvalid = 0;
        $mappedMismatchSkipped = 0;
        $mappedMissingSkipped = 0;
        $recovered = 0;
        $recoveryAmbiguousSkipped = 0;
        $created = 0;
        $wouldCreate = 0;
        $dependencyFailures = 0;

        foreach ($sourceRepeatingInvoices as $sourceInvoice) {
            $processed++;

            $sourceId = $sourceInvoice['RepeatingInvoiceID'] ?? $sourceInvoice['ID'] ?? null;
            $type = strtoupper(trim((string) ($sourceInvoice['Type'] ?? '')));
            $contactName = trim((string) ($sourceInvoice['Contact']['Name'] ?? ''));
            $reference = trim((string) ($sourceInvoice['Reference'] ?? ''));

            if (!$sourceId || $type === '' || $contactName === '') {
                $this->error('Invalid source repeating invoice.');
                $this->line(json_encode($sourceInvoice, JSON_PRETTY_PRINT | JSON_UNESCAPED_SLASHES));
                return Command::FAILURE;
            }

            $this->newLine();
            $this->info("Next repeating invoice: {$contactName} | {$type} | {$sourceId}");

            $payloadResult = $this->buildPayloadForDestination(
                $sourceInvoice,
                $destinationRole,
                $mapper,
                $source,
                $destination,
                $destinationAccountsByCode,
                $destinationTaxTypes,
                $destinationItemsByCode,
                $destinationTrackingByCategoryId,
                $destinationTrackingByOptionId
            );

            if (!$payloadResult['ok']) {
                $dependencyFailures++;
                $this->error("Dependency validation failed for repeating invoice: {$contactName} | {$sourceId}");
                $this->line($payloadResult['error']);

                if ($dryRun) {
                    continue;
                }

                return Command::FAILURE;
            }

            $payload = $payloadResult['payload'];
            $payloadInvoice = $payload['RepeatingInvoices'][0];
            $fingerprint = $this->buildPayloadFingerprint($payloadInvoice);

            $mappedId = $destinationRole === 'test'
                ? $mapper->getTestId(self::ENTITY, $sourceId)
                : $mapper->getTargetId(self::ENTITY, $sourceId);

            if ($mappedId) {
                $mappedInvoice = $destinationRepeatingById->get($mappedId);

                if ($mappedInvoice) {
                    $destinationFingerprint = $this->buildDestinationFingerprint($mappedInvoice, $destinationTrackingByOptionId);

                    if ($destinationFingerprint === $fingerprint) {
                        $mappedValid++;
                        $this->info("Mapped already → Valid: {$contactName} ({$mappedId})");
                        continue;
                    }

                    $reason = $this->explainFingerprintMismatch(
                        $payloadInvoice,
                        $mappedInvoice,
                        $destinationTrackingByOptionId
                    );

                    $mappedInvalid++;
                    $mappedMismatchSkipped++;
                    $this->warn("Mapped target {$reason} → skipped: {$contactName} ({$mappedId})");
                    $this->line("Source RepeatingInvoiceID: {$sourceId}");
                    $this->line("Target RepeatingInvoiceID: {$mappedId}");

                    Log::warning('Repeating invoice mapped ID failed strict validation', [
                        'destination_role' => $destinationRole,
                        'source_repeating_invoice_id' => $sourceId,
                        'mapped_target_id' => $mappedId,
                        'contact_name' => $contactName,
                        'type' => $type,
                        'reference' => $reference,
                        'reason' => $reason,
                    ]);

                    continue;
                }

                $mappedInvalid++;
                $mappedMissingSkipped++;
                $this->warn("Mapped target repeating invoice not found in destination → skipped: {$contactName} ({$mappedId})");
                $this->line("Source RepeatingInvoiceID: {$sourceId}");
                $this->line("Missing Target RepeatingInvoiceID: {$mappedId}");

                Log::warning('Repeating invoice mapped ID not found in destination index', [
                    'destination_role' => $destinationRole,
                    'source_repeating_invoice_id' => $sourceId,
                    'mapped_target_id' => $mappedId,
                    'contact_name' => $contactName,
                    'type' => $type,
                    'reference' => $reference,
                    'reason' => 'mapped target missing; skipped, not created',
                ]);

                continue;
            }

            $existingDestination = $destinationRepeatingByFingerprint->get($fingerprint);

            if ($existingDestination && $existingDestination->count() === 1) {
                $matchedDestination = $existingDestination->first();

                if ($dryRun) {
                    $recovered++;
                    $this->info("Recoverable by fingerprint → {$contactName} ({$matchedDestination['RepeatingInvoiceID']})");
                    continue;
                }

                $this->storeMapping(
                    $mapper,
                    $destinationRole,
                    $source,
                    $destination,
                    $sourceId,
                    $matchedDestination['RepeatingInvoiceID'],
                    $contactName
                );

                $verifiedId = $destinationRole === 'test'
                    ? $mapper->getTestId(self::ENTITY, $sourceId)
                    : $mapper->getTargetId(self::ENTITY, $sourceId);

                if (!$verifiedId || $verifiedId !== $matchedDestination['RepeatingInvoiceID']) {
                    $this->error("Mapping verification failed for recovered repeating invoice: {$contactName}");
                    return Command::FAILURE;
                }

                $recovered++;
                $this->info("Recovered + mapped → {$contactName} ({$matchedDestination['RepeatingInvoiceID']})");
                continue;
            }

            if ($existingDestination && $existingDestination->count() > 1) {
                $recoveryAmbiguousSkipped++;
                $this->warn("Ambiguous repeating invoice recovery detected → skipped: {$contactName}");
                Log::warning('Ambiguous repeating invoice recovery detected', [
                    'destination_role' => $destinationRole,
                    'source_repeating_invoice_id' => $sourceId,
                    'contact_name' => $contactName,
                    'type' => $type,
                    'reference' => $reference,
                ]);
                continue;
            }

            if ($dryRun) {
                $wouldCreate++;
                $this->info("Would create → {$contactName} ({$sourceId})");
                continue;
            }

            $post = $this->postRepeatingInvoicesWithRetry($destination, $payload, $contactName, $sourceId);
            $destinationId = $post->json('RepeatingInvoices.0.RepeatingInvoiceID')
                ?? $post->json('RepeatingInvoices.0.ID')
                ?? null;

            if ($post->successful() && $destinationId) {
                $this->storeMapping(
                    $mapper,
                    $destinationRole,
                    $source,
                    $destination,
                    $sourceId,
                    $destinationId,
                    $contactName
                );

                $verifiedId = $destinationRole === 'test'
                    ? $mapper->getTestId(self::ENTITY, $sourceId)
                    : $mapper->getTargetId(self::ENTITY, $sourceId);

                if (!$verifiedId || $verifiedId !== $destinationId) {
                    $this->error("Mapping verification failed for created repeating invoice: {$contactName}");
                    return Command::FAILURE;
                }

                $createdInvoice = $post->json('RepeatingInvoices.0');
                if (is_array($createdInvoice)) {
                    $destinationRepeatingInvoices->push($createdInvoice);
                    [$destinationRepeatingById, $destinationRepeatingByFingerprint] = $this->buildDestinationRepeatingIndexes(
                        $destinationRepeatingInvoices,
                        $destinationTrackingByOptionId
                    );
                }

                $created++;
                $this->info("Created + mapped → {$contactName} ({$destinationId})");
                continue;
            }

            $this->warn("Create not completed -> refreshing destination repeating invoices index and retrying recovery: {$contactName}");

            $destinationRepeatingInvoices = $this->fetchRepeatingInvoices($destination, strtoupper($destinationRole));
            [$destinationRepeatingById, $destinationRepeatingByFingerprint] = $this->buildDestinationRepeatingIndexes(
                $destinationRepeatingInvoices,
                $destinationTrackingByOptionId
            );

            $existingDestination = $destinationRepeatingByFingerprint->get($fingerprint);

            if ($existingDestination && $existingDestination->count() === 1) {
                $matchedDestination = $existingDestination->first();

                $this->storeMapping(
                    $mapper,
                    $destinationRole,
                    $source,
                    $destination,
                    $sourceId,
                    $matchedDestination['RepeatingInvoiceID'],
                    $contactName
                );

                $verifiedId = $destinationRole === 'test'
                    ? $mapper->getTestId(self::ENTITY, $sourceId)
                    : $mapper->getTargetId(self::ENTITY, $sourceId);

                if (!$verifiedId || $verifiedId !== $matchedDestination['RepeatingInvoiceID']) {
                    $this->error("Mapping verification failed after recovery for repeating invoice: {$contactName}");
                    return Command::FAILURE;
                }

                $recovered++;
                $this->info("Recovered after refresh + mapped → {$contactName} ({$matchedDestination['RepeatingInvoiceID']})");
                continue;
            }

            $this->error("Unable to create or recover repeating invoice: {$contactName}");
            $this->line('HTTP Status: ' . $post->status());
            $this->line($post->body());

            Log::error('Repeating invoice sync failed', [
                'contact_name' => $contactName,
                'source_repeating_invoice_id' => $sourceId,
                'payload' => $payload,
                'status' => $post->status(),
                'response' => $post->body(),
            ]);

            return Command::FAILURE;
        }

        $this->newLine();
        $this->info('Done.');
        $this->line("Filtered source ACCREC (total before first/batch/limit): {$fullFilteredSourceCount}");
        $this->line("Selected source ACCREC (this run): {$selectedSourceCount}");
        $this->line("Filtered target ACCREC: {$filteredTargetCount}");
        $this->line("Processed ACCREC: {$processed}");
        $this->line("Skipped ACCPAY: {$skippedAccPay}");
        $this->line("Valid mapped: {$mappedValid}");
        $this->line("Invalid mapped found: {$mappedInvalid}");
        $this->line("Mapped mismatch skipped: {$mappedMismatchSkipped}");
        $this->line("Mapped missing skipped: {$mappedMissingSkipped}");
        $this->line("Recoverable by fingerprint: {$recovered}");
        $this->line("Ambiguous recovery skipped: {$recoveryAmbiguousSkipped}");
        $this->line("Dependency validation failures: {$dependencyFailures}");

        if ($dryRun) {
            $this->line("Would create: {$wouldCreate}");
        } else {
            $this->line("Created + mapped: {$created}");
        }

        return Command::SUCCESS;
    }

    private function buildPayloadForDestination(
        array $sourceInvoice,
        string $destinationRole,
        XeroIdMapper $mapper,
        XeroOrganisation $source,
        XeroOrganisation $destination,
        Collection $destinationAccountsByCode,
        array $destinationTaxTypes,
        Collection $destinationItemsByCode,
        Collection $destinationTrackingByCategoryId,
        Collection $destinationTrackingByOptionId
    ): array {
        $sourceId = $sourceInvoice['RepeatingInvoiceID'] ?? $sourceInvoice['ID'] ?? null;
        $contact = $sourceInvoice['Contact'] ?? [];
        $sourceContactId = $contact['ContactID'] ?? null;
        $contactName = trim((string) ($contact['Name'] ?? ''));

        if (!$sourceContactId || $contactName === '') {
            return ['ok' => false, 'error' => 'Source repeating invoice has no valid Contact.ContactID / Contact.Name'];
        }

        $targetContactId = $destinationRole === 'test'
            ? $mapper->getTestId('contact', $sourceContactId)
            : $mapper->getTargetId('contact', $sourceContactId);

        if (!$targetContactId) {
            return ['ok' => false, 'error' => "Missing contact mapping for source contact {$contactName} ({$sourceContactId})"];
        }

        $type = strtoupper(trim((string) ($sourceInvoice['Type'] ?? '')));
        if ($type !== 'ACCREC') {
            return ['ok' => false, 'error' => "Only ACCREC is supported for create. Found {$type}"];
        }

        $payloadInvoice = [
            'Type' => 'ACCREC',
            'Contact' => [
                'ContactID' => $targetContactId,
            ],
            'Status' => strtoupper(trim((string) ($sourceInvoice['Status'] ?? 'DRAFT'))),
            'LineAmountTypes' => $sourceInvoice['LineAmountTypes'] ?? 'Exclusive',
            'LineItems' => [],
            'Schedule' => $this->sanitizeSchedule(
                $sourceInvoice['Schedule'] ?? [],
                $sourceId,
                $contactName
            ),
            'IncludePDF' => (bool) ($sourceInvoice['IncludePDF'] ?? false),
            'ApprovedForSending' => (bool) ($sourceInvoice['ApprovedForSending'] ?? false),
            'CurrencyCode' => $sourceInvoice['CurrencyCode'] ?? null,
            'Reference' => $sourceInvoice['Reference'] ?? null,
        ];

        if (!in_array($payloadInvoice['Status'], ['DRAFT', 'AUTHORISED'], true)) {
            return ['ok' => false, 'error' => "Unsupported repeating invoice status for create: {$payloadInvoice['Status']}"];
        }

        if (empty($payloadInvoice['Schedule'])) {
            return ['ok' => false, 'error' => "Repeating invoice {$sourceId} has no usable Schedule"];
        }

        $sourceBrandingThemeId = $sourceInvoice['BrandingThemeID']
            ?? ($sourceInvoice['BrandingTheme']['BrandingThemeID'] ?? null);

        if ($sourceBrandingThemeId) {
            $targetBrandingThemeId = $destinationRole === 'test'
                ? $mapper->getTestId('branding_theme', $sourceBrandingThemeId)
                : $mapper->getTargetId('branding_theme', $sourceBrandingThemeId);

            if (!$targetBrandingThemeId && !$this->option('allow-missing-branding')) {
                return ['ok' => false, 'error' => "Missing branding theme mapping for source BrandingThemeID {$sourceBrandingThemeId}"];
            }

            if ($targetBrandingThemeId) {
                $payloadInvoice['BrandingThemeID'] = $targetBrandingThemeId;
            }
        }

        $lineItems = collect($sourceInvoice['LineItems'] ?? []);
        if ($lineItems->isEmpty()) {
            return ['ok' => false, 'error' => "Repeating invoice {$sourceId} has no LineItems"];
        }

        foreach ($lineItems as $index => $sourceLine) {
            $lineNumber = $index + 1;
            $cleanLine = $this->sanitizeRepeatingInvoiceLine($sourceLine);

            if (empty($cleanLine['Description']) && empty($cleanLine['ItemCode'])) {
                return ['ok' => false, 'error' => "Line {$lineNumber} has neither Description nor ItemCode"];
            }

            $isDescriptionOnlyLine = $this->isDescriptionOnlyLine($cleanLine);

            if (!$isDescriptionOnlyLine) {
                $itemCode = trim((string) ($cleanLine['ItemCode'] ?? ''));

                if ($itemCode !== '' && !$destinationItemsByCode->has($itemCode)) {
                    return ['ok' => false, 'error' => "Line {$lineNumber} references missing destination item code {$itemCode}"];
                }

                $accountCode = trim((string) ($cleanLine['AccountCode'] ?? ''));
                if ($accountCode !== '') {
                    if (!$destinationAccountsByCode->has($accountCode)) {
                        return ['ok' => false, 'error' => "Line {$lineNumber} references missing destination account code {$accountCode}"];
                    }
                } else {
                    unset($cleanLine['AccountCode']);
                }

                $taxType = strtoupper(trim((string) ($cleanLine['TaxType'] ?? '')));

                if ($taxType === 'NONE') {
                    $cleanLine['TaxType'] = 'NONE';
                } elseif ($taxType !== '') {
                    if (!isset($destinationTaxTypes[$taxType])) {
                        return ['ok' => false, 'error' => "Line {$lineNumber} references missing destination tax type {$taxType}"];
                    }

                    $cleanLine['TaxType'] = $taxType;
                } else {
                    unset($cleanLine['TaxType']);
                }

                $cleanLine = $this->normalizeDiscountFields($cleanLine);

                if (
                    isset($cleanLine['Quantity'], $cleanLine['UnitAmount']) &&
                    (float) $cleanLine['Quantity'] != 0.0
                ) {
                    $quantity = (float) $cleanLine['Quantity'];
                    $unitAmount = (float) $cleanLine['UnitAmount'];
                    $lineAmount = isset($cleanLine['LineAmount'])
                        ? round((float) $cleanLine['LineAmount'], 2)
                        : null;
                    $discountRate = (float) ($cleanLine['DiscountRate'] ?? 0);

                    if (abs($discountRate - 100.0) < 0.00001) {
                        $negativeAmount = round($quantity * $unitAmount, 2) * -1;

                        $cleanLine['Quantity'] = 1;
                        $cleanLine['UnitAmount'] = $negativeAmount;
                        unset(
                            $cleanLine['LineAmount'],
                            $cleanLine['DiscountRate'],
                            $cleanLine['DiscountEnteredAsPercent']
                        );

                        $this->warn(
                            "100% discount line converted to fixed negative amount for {$contactName} ({$sourceId}) on line {$lineNumber}."
                        );
                    } elseif (
                        $lineAmount !== null &&
                        abs($discountRate) > 0.00001
                    ) {
                        $expectedLineAmount = round(
                            $quantity * $unitAmount * (1 - ($discountRate / 100)),
                            2
                        );

                        if (abs($expectedLineAmount - $lineAmount) > 0.009) {
                            unset($cleanLine['LineAmount']);

                            $this->warn(
                                "Discounted line amount omitted for {$contactName} ({$sourceId}) on line {$lineNumber}: " .
                                    "source LineAmount did not match Quantity × UnitAmount × (1 - DiscountRate), so Xero will recalculate it."
                            );
                        }
                    } elseif ($lineAmount !== null) {
                        $expectedLineAmount = round($quantity * $unitAmount, 2);

                        if (abs($expectedLineAmount - $lineAmount) > 0.009) {
                            $cleanLine['UnitAmount'] = round($lineAmount / $quantity, 4);
                            unset($cleanLine['LineAmount']);

                            $this->warn(
                                "Line rounding adjusted for {$contactName} ({$sourceId}) on line {$lineNumber}: " .
                                    "UnitAmount recalculated from LineAmount/Quantity because Xero rejected inconsistent line totals."
                            );
                        }
                    }
                }
            } else {
                unset(
                    $cleanLine['AccountCode'],
                    $cleanLine['TaxType'],
                    $cleanLine['Quantity'],
                    $cleanLine['UnitAmount'],
                    $cleanLine['ItemCode'],
                    $cleanLine['Tracking'],
                    $cleanLine['DiscountRate'],
                    $cleanLine['DiscountEnteredAsPercent'],
                    $cleanLine['LineAmount']
                );
            }

            $trackingResult = $this->mapTrackingForDestination(
                $sourceLine['Tracking'] ?? [],
                $destinationRole,
                $mapper,
                $destinationTrackingByCategoryId,
                $destinationTrackingByOptionId,
                $lineNumber,
                $isDescriptionOnlyLine
            );

            if (!$trackingResult['ok']) {
                return ['ok' => false, 'error' => $trackingResult['error']];
            }

            if (!empty($trackingResult['tracking'])) {
                $cleanLine['Tracking'] = $trackingResult['tracking'];
            } else {
                unset($cleanLine['Tracking']);
            }

            unset(
                $cleanLine['LineItemID'],
                $cleanLine['AccountID'],
                $cleanLine['TaxAmount'],
                $cleanLine['RepeatingInvoiceID'],
                $cleanLine['ValidationErrors']
            );

            $payloadInvoice['LineItems'][] = $cleanLine;
        }

        $payloadInvoice = array_filter($payloadInvoice, function ($value) {
            if (is_array($value)) {
                return !empty($value);
            }

            return $value !== null && $value !== '';
        });

        return [
            'ok' => true,
            'payload' => [
                'RepeatingInvoices' => [$payloadInvoice],
            ],
        ];
    }

    private function mapTrackingForDestination(
        array $sourceTracking,
        string $destinationRole,
        XeroIdMapper $mapper,
        Collection $destinationTrackingByCategoryId,
        Collection $destinationTrackingByOptionId,
        int $lineNumber,
        bool $isDescriptionOnlyLine = false
    ): array {
        if ($isDescriptionOnlyLine || empty($sourceTracking)) {
            return ['ok' => true, 'tracking' => []];
        }

        $result = [];

        foreach ($sourceTracking as $trackingEntry) {
            $sourceOptionId = $trackingEntry['TrackingOptionID'] ?? null;
            $sourceCategoryId = $trackingEntry['TrackingCategoryID'] ?? null;

            if ($sourceOptionId) {
                $targetOptionId = $destinationRole === 'test'
                    ? $mapper->getTestId('tracking_option', $sourceOptionId)
                    : $mapper->getTargetId('tracking_option', $sourceOptionId);

                if (!$targetOptionId) {
                    return ['ok' => false, 'error' => "Line {$lineNumber} tracking option mapping missing for source TrackingOptionID {$sourceOptionId}"];
                }

                $targetOption = $destinationTrackingByOptionId->get($targetOptionId);
                if (!$targetOption) {
                    return ['ok' => false, 'error' => "Line {$lineNumber} mapped target tracking option not found: {$targetOptionId}"];
                }

                $targetCategoryId = $targetOption['_parent_category_id'] ?? null;
                $targetCategory = $targetCategoryId ? $destinationTrackingByCategoryId->get($targetCategoryId) : null;

                if (!$targetCategory) {
                    return ['ok' => false, 'error' => "Line {$lineNumber} target tracking category not found for option {$targetOptionId}"];
                }

                $result[] = [
                    'Name' => $targetCategory['Name'],
                    'Option' => $targetOption['Name'],
                ];
                continue;
            }

            if ($sourceCategoryId && !empty($trackingEntry['Name']) && !empty($trackingEntry['Option'])) {
                $targetCategoryId = $destinationRole === 'test'
                    ? $mapper->getTestId('tracking_category', $sourceCategoryId)
                    : $mapper->getTargetId('tracking_category', $sourceCategoryId);

                if (!$targetCategoryId) {
                    return ['ok' => false, 'error' => "Line {$lineNumber} tracking category mapping missing for source TrackingCategoryID {$sourceCategoryId}"];
                }

                $targetCategory = $destinationTrackingByCategoryId->get($targetCategoryId);
                if (!$targetCategory) {
                    return ['ok' => false, 'error' => "Line {$lineNumber} target tracking category not found: {$targetCategoryId}"];
                }

                $normalizedOption = $this->normalizeName((string) $trackingEntry['Option']);
                $matchedOption = collect($targetCategory['Options'] ?? [])->first(function ($option) use ($normalizedOption) {
                    return $this->normalizeName((string) ($option['Name'] ?? '')) === $normalizedOption;
                });

                if (!$matchedOption) {
                    return ['ok' => false, 'error' => "Line {$lineNumber} target tracking option not found by option name {$trackingEntry['Option']}"];
                }

                $result[] = [
                    'Name' => $targetCategory['Name'],
                    'Option' => $matchedOption['Name'],
                ];
                continue;
            }

            return ['ok' => false, 'error' => "Line {$lineNumber} has unsupported tracking payload; missing IDs/names needed for strict mapping"];
        }

        return ['ok' => true, 'tracking' => $result];
    }

    private function buildDestinationRepeatingIndexes(Collection $invoices, Collection $destinationTrackingByOptionId): array
    {
        $byId = $invoices
            ->filter(fn($invoice) => !empty($invoice['RepeatingInvoiceID']))
            ->keyBy('RepeatingInvoiceID');

        $byFingerprint = $invoices
            ->filter(fn($invoice) => !empty($invoice['RepeatingInvoiceID']))
            ->groupBy(fn($invoice) => $this->buildDestinationFingerprint($invoice, $destinationTrackingByOptionId));

        return [$byId, $byFingerprint];
    }

    private function buildPayloadFingerprint(array $payloadInvoice): string
    {
        return sha1(json_encode($this->buildPayloadFingerprintData($payloadInvoice)));
    }

    private function buildDestinationFingerprint(array $destinationInvoice, Collection $destinationTrackingByOptionId): string
    {
        return sha1(json_encode($this->buildDestinationFingerprintData($destinationInvoice, $destinationTrackingByOptionId)));
    }

    private function buildPayloadFingerprintData(array $payloadInvoice): array
    {
        $schedule = $payloadInvoice['Schedule'] ?? [];

        $lineItems = collect($payloadInvoice['LineItems'] ?? [])
            ->map(function ($line) {
                $tracking = collect($line['Tracking'] ?? [])
                    ->map(function ($track) {
                        return [
                            'Name' => $this->normalizeName((string) ($track['Name'] ?? '')),
                            'Option' => $this->normalizeName((string) ($track['Option'] ?? '')),
                        ];
                    })
                    ->sortBy(fn($track) => ($track['Name'] ?? '') . '|' . ($track['Option'] ?? ''))
                    ->values()
                    ->all();

                return [
                    'Description' => $this->normalizeTextValue((string) ($line['Description'] ?? '')),
                    'Quantity' => $this->normalizeNumericValue($line['Quantity'] ?? ''),
                    'UnitAmount' => $this->normalizeNumericValue($line['UnitAmount'] ?? ''),
                    'LineAmount' => $this->normalizeNumericValue($line['LineAmount'] ?? ''),
                    'AccountCode' => trim((string) ($line['AccountCode'] ?? '')),
                    'TaxType' => trim((string) ($line['TaxType'] ?? '')),
                    'ItemCode' => trim((string) ($line['ItemCode'] ?? '')),
                    'DiscountRate' => $this->normalizeNumericValue($line['DiscountRate'] ?? ''),
                    'DiscountEnteredAsPercent' => (bool) ($line['DiscountEnteredAsPercent'] ?? false),
                    'Tracking' => $tracking,
                ];
            })
            ->values()
            ->all();

        return [
            'Type' => strtoupper(trim((string) ($payloadInvoice['Type'] ?? ''))),
            'ContactID' => trim((string) ($payloadInvoice['Contact']['ContactID'] ?? '')),
            'Status' => strtoupper(trim((string) ($payloadInvoice['Status'] ?? ''))),
            'LineAmountTypes' => strtoupper(trim((string) ($payloadInvoice['LineAmountTypes'] ?? ''))),
            'Reference' => $this->normalizeTextValue((string) ($payloadInvoice['Reference'] ?? '')),
            'IncludePDF' => (bool) ($payloadInvoice['IncludePDF'] ?? false),
            'ApprovedForSending' => (bool) ($payloadInvoice['ApprovedForSending'] ?? false),
            'CurrencyCode' => trim((string) ($payloadInvoice['CurrencyCode'] ?? '')),
            'BrandingThemeID' => trim((string) ($payloadInvoice['BrandingThemeID'] ?? '')),
            'Schedule' => [
                'Period' => $schedule['Period'] ?? null,
                'Unit' => strtoupper(trim((string) ($schedule['Unit'] ?? ''))),
                'DueDate' => $schedule['DueDate'] ?? null,
                'DueDateType' => strtoupper(trim((string) ($schedule['DueDateType'] ?? ''))),
                'StartDate' => $this->formatXeroDateToYmd($schedule['StartDate'] ?? null) ?? trim((string) ($schedule['StartDate'] ?? '')),
                'EndDate' => $this->formatXeroDateToYmd($schedule['EndDate'] ?? null),
            ],
            'LineItems' => $lineItems,
        ];
    }

    private function buildDestinationFingerprintData(array $destinationInvoice, Collection $destinationTrackingByOptionId): array
    {
        $schedule = $destinationInvoice['Schedule'] ?? [];
        $contactId = $destinationInvoice['Contact']['ContactID'] ?? '';
        $brandingThemeId = $destinationInvoice['BrandingThemeID']
            ?? ($destinationInvoice['BrandingTheme']['BrandingThemeID'] ?? '');

        $lineItems = collect($destinationInvoice['LineItems'] ?? [])
            ->map(function ($line) use ($destinationTrackingByOptionId) {
                $tracking = collect($line['Tracking'] ?? [])
                    ->map(function ($track) use ($destinationTrackingByOptionId) {
                        $name = $track['Name'] ?? null;
                        $option = $track['Option'] ?? null;

                        if ((!$name || !$option) && !empty($track['TrackingOptionID'])) {
                            $targetOption = $destinationTrackingByOptionId->get($track['TrackingOptionID']);

                            if ($targetOption) {
                                $option = $targetOption['Name'] ?? $option;
                                $name = $targetOption['_parent_category_name'] ?? $name;
                            }
                        }

                        return [
                            'Name' => $this->normalizeName((string) ($name ?? '')),
                            'Option' => $this->normalizeName((string) ($option ?? '')),
                        ];
                    })
                    ->sortBy(fn($track) => ($track['Name'] ?? '') . '|' . ($track['Option'] ?? ''))
                    ->values()
                    ->all();

                return [
                    'Description' => $this->normalizeTextValue((string) ($line['Description'] ?? '')),
                    'Quantity' => $this->normalizeNumericValue($line['Quantity'] ?? ''),
                    'UnitAmount' => $this->normalizeNumericValue($line['UnitAmount'] ?? ''),
                    'LineAmount' => $this->normalizeNumericValue($line['LineAmount'] ?? ''),
                    'AccountCode' => trim((string) ($line['AccountCode'] ?? '')),
                    'TaxType' => trim((string) ($line['TaxType'] ?? '')),
                    'ItemCode' => trim((string) ($line['ItemCode'] ?? '')),
                    'DiscountRate' => $this->normalizeNumericValue($line['DiscountRate'] ?? ''),
                    'DiscountEnteredAsPercent' => (bool) ($line['DiscountEnteredAsPercent'] ?? false),
                    'Tracking' => $tracking,
                ];
            })
            ->values()
            ->all();

        return [
            'Type' => strtoupper(trim((string) ($destinationInvoice['Type'] ?? ''))),
            'ContactID' => trim((string) $contactId),
            'Status' => strtoupper(trim((string) ($destinationInvoice['Status'] ?? ''))),
            'LineAmountTypes' => strtoupper(trim((string) ($destinationInvoice['LineAmountTypes'] ?? ''))),
            'Reference' => $this->normalizeTextValue((string) ($destinationInvoice['Reference'] ?? '')),
            'IncludePDF' => (bool) ($destinationInvoice['IncludePDF'] ?? false),
            'ApprovedForSending' => (bool) ($destinationInvoice['ApprovedForSending'] ?? false),
            'CurrencyCode' => trim((string) ($destinationInvoice['CurrencyCode'] ?? '')),
            'BrandingThemeID' => trim((string) $brandingThemeId),
            'Schedule' => [
                'Period' => $schedule['Period'] ?? null,
                'Unit' => strtoupper(trim((string) ($schedule['Unit'] ?? ''))),
                'DueDate' => $schedule['DueDate'] ?? null,
                'DueDateType' => strtoupper(trim((string) ($schedule['DueDateType'] ?? ''))),
                'StartDate' => $this->formatXeroDateToYmd($schedule['StartDate'] ?? null),
                'EndDate' => $this->formatXeroDateToYmd($schedule['EndDate'] ?? null),
            ],
            'LineItems' => $lineItems,
        ];
    }

    private function filterSourceRepeatingInvoices(Collection $invoices): Collection
    {
        $today = Carbon::today();
        $nextDateFilter = trim((string) ($this->option('next-date') ?? ''));

        return $invoices->filter(function (array $invoice) use ($today, $nextDateFilter) {
            $sourceId = $invoice['RepeatingInvoiceID'] ?? $invoice['ID'] ?? null;
            if (!$sourceId) {
                return false;
            }

            if ($this->option('source-id') && $sourceId !== $this->option('source-id')) {
                return false;
            }

            if ($this->option('name')) {
                $contactName = trim((string) ($invoice['Contact']['Name'] ?? ''));
                if ($contactName !== (string) $this->option('name')) {
                    return false;
                }
            }

            $type = strtoupper(trim((string) ($invoice['Type'] ?? '')));
            if ($type !== 'ACCREC') {
                return false;
            }

            $status = strtoupper(trim((string) ($invoice['Status'] ?? 'UNKNOWN')));
            if ($status === 'DELETED') {
                return false;
            }

            $schedule = $invoice['Schedule'] ?? [];
            $nextDateYmd = $this->formatXeroDateToYmd(
                $schedule['NextScheduledDateString']
                    ?? $schedule['NextScheduledDate']
                    ?? null
            );

            if ($nextDateFilter !== '' && $nextDateYmd !== $nextDateFilter) {
                return false;
            }

            if ($this->option('all-non-deleted')) {
                return true;
            }

            $nextDate = $this->parseXeroDate($schedule['NextScheduledDate'] ?? $schedule['NextScheduledDateString'] ?? null);
            $endDate = $this->parseXeroDate($schedule['EndDate'] ?? null);

            return $nextDate
                && $nextDate->gte($today)
                && (!$endDate || $endDate->gte($today));
        })->values();
    }

    private function filterDestinationRepeatingInvoices(Collection $invoices): Collection
    {
        $today = Carbon::today();
        $nextDateFilter = trim((string) ($this->option('next-date') ?? ''));

        return $invoices->filter(function (array $invoice) use ($today, $nextDateFilter) {
            $type = strtoupper(trim((string) ($invoice['Type'] ?? '')));
            if ($type !== 'ACCREC') {
                return false;
            }

            $status = strtoupper(trim((string) ($invoice['Status'] ?? 'UNKNOWN')));
            if ($status === 'DELETED') {
                return false;
            }

            if ($this->option('name')) {
                $contactName = trim((string) ($invoice['Contact']['Name'] ?? ''));
                if ($contactName !== (string) $this->option('name')) {
                    return false;
                }
            }

            $schedule = $invoice['Schedule'] ?? [];
            $nextDateYmd = $this->formatXeroDateToYmd(
                $schedule['NextScheduledDateString']
                    ?? $schedule['NextScheduledDate']
                    ?? null
            );

            if ($nextDateFilter !== '' && $nextDateYmd !== $nextDateFilter) {
                return false;
            }

            if ($this->option('all-non-deleted')) {
                return true;
            }

            $nextDate = $this->parseXeroDate($schedule['NextScheduledDate'] ?? $schedule['NextScheduledDateString'] ?? null);
            $endDate = $this->parseXeroDate($schedule['EndDate'] ?? null);

            return $nextDate
                && $nextDate->gte($today)
                && (!$endDate || $endDate->gte($today));
        })->values();
    }

    private function sanitizeSchedule(array $schedule, ?string $sourceId = null, ?string $contactName = null): array
    {
        $dueDateType = $schedule['DueDateType'] ?? null;
        $dueDate = $schedule['DueDate'] ?? null;

        if (
            strtoupper((string) $dueDateType) === 'DAYSAFTERBILLDATE'
            && ((int) $dueDate) === 0
        ) {
            $this->warn(
                'DueDate adjusted from 0 to 1 for repeating invoice'
                    . ($contactName ? " {$contactName}" : '')
                    . ($sourceId ? " ({$sourceId})" : '')
                    . ' because Xero does not accept 0 for DAYSAFTERBILLDATE.'
            );

            $dueDate = 1;
        }

        $clean = [
            'Period' => $schedule['Period'] ?? null,
            'Unit' => $schedule['Unit'] ?? null,
            'DueDate' => $dueDate,
            'DueDateType' => $dueDateType,
            'StartDate' => $this->resolveTakeoverStartDate($schedule),
            'EndDate' => $this->formatXeroDateToYmd($schedule['EndDate'] ?? null),
        ];

        return array_filter($clean, function ($value) {
            return $value !== null && $value !== '';
        });
    }

    private function resolveTakeoverStartDate(array $schedule): ?string
    {
        $candidate = $this->formatXeroDateToYmd(
            $schedule['NextScheduledDateString']
                ?? $schedule['NextScheduledDate']
                ?? $schedule['StartDate']
                ?? null
        );

        if (!$candidate) {
            return null;
        }

        $period = (int) ($schedule['Period'] ?? 1);
        $period = $period > 0 ? $period : 1;

        $unit = strtoupper(trim((string) ($schedule['Unit'] ?? 'MONTHLY')));

        return $this->moveDateToFirstOccurrenceOnOrAfterTakeover($candidate, $period, $unit);
    }

    private function moveDateToFirstOccurrenceOnOrAfterTakeover(string $candidateDate, int $period, string $unit): string
    {
        $takeover = Carbon::createFromFormat('Y-m-d', self::TAKEOVER_DATE)->startOfDay();
        $candidate = Carbon::createFromFormat('Y-m-d', $candidateDate)->startOfDay();

        if ($candidate->gte($takeover)) {
            return $candidate->format('Y-m-d');
        }

        $safeGuard = 0;

        while ($candidate->lt($takeover) && $safeGuard < 500) {
            if ($unit === 'WEEKLY') {
                $candidate->addWeeks($period);
            } else {
                $candidate->addMonthsNoOverflow($period);
            }

            $safeGuard++;
        }

        return $candidate->format('Y-m-d');
    }

    private function sanitizeRepeatingInvoiceLine(array $line): array
    {
        return collect($line)
            ->except([
                'LineItemID',
                'TaxAmount',
                'AccountID',
                'RepeatingInvoiceID',
                'ValidationErrors',
            ])
            ->map(fn($value) => $this->sanitizeValue($value))
            ->filter(function ($value) {
                if (is_array($value)) {
                    return !empty($value);
                }

                return $value !== null && $value !== '';
            })
            ->toArray();
    }

    private function sanitizeValue($value)
    {
        if (is_array($value)) {
            $isList = array_keys($value) === range(0, count($value) - 1);

            if ($isList) {
                $cleanedList = [];

                foreach ($value as $item) {
                    $cleanedItem = $this->sanitizeValue($item);

                    if (is_array($cleanedItem) && empty($cleanedItem)) {
                        continue;
                    }

                    if ($cleanedItem === null || $cleanedItem === '') {
                        continue;
                    }

                    $cleanedList[] = $cleanedItem;
                }

                return $cleanedList;
            }

            $cleanedAssoc = [];

            foreach ($value as $key => $item) {
                $cleanedItem = $this->sanitizeValue($item);

                if (is_array($cleanedItem) && empty($cleanedItem)) {
                    continue;
                }

                if ($cleanedItem === null || $cleanedItem === '') {
                    continue;
                }

                $cleanedAssoc[$key] = $cleanedItem;
            }

            return $cleanedAssoc;
        }

        return $value;
    }

    private function isDescriptionOnlyLine(array $cleanLine): bool
    {
        $description = trim((string) ($cleanLine['Description'] ?? ''));
        $itemCode = trim((string) ($cleanLine['ItemCode'] ?? ''));
        $accountCode = trim((string) ($cleanLine['AccountCode'] ?? ''));
        $taxType = trim((string) ($cleanLine['TaxType'] ?? ''));
        $quantity = $cleanLine['Quantity'] ?? null;
        $unitAmount = $cleanLine['UnitAmount'] ?? null;

        return $description !== ''
            && $itemCode === ''
            && $accountCode === ''
            && $taxType === ''
            && ($quantity === null || $quantity === '')
            && ($unitAmount === null || $unitAmount === '');
    }

    private function fetchRepeatingInvoices(XeroOrganisation $organisation, string $label): Collection
    {
        $response = $this->getWithRetry($organisation, $label, 'https://api.xero.com/api.xro/2.0/RepeatingInvoices', 'repeating invoices');
        return collect($response->json('RepeatingInvoices', []));
    }

    private function fetchAccounts(XeroOrganisation $organisation, string $label): Collection
    {
        $response = $this->getWithRetry($organisation, $label, 'https://api.xero.com/api.xro/2.0/Accounts', 'accounts');
        return collect($response->json('Accounts', []));
    }

    private function fetchTaxRates(XeroOrganisation $organisation, string $label): Collection
    {
        $response = $this->getWithRetry($organisation, $label, 'https://api.xero.com/api.xro/2.0/TaxRates', 'tax rates');
        return collect($response->json('TaxRates', []));
    }

    private function fetchItems(XeroOrganisation $organisation, string $label): Collection
    {
        $response = $this->getWithRetry($organisation, $label, 'https://api.xero.com/api.xro/2.0/Items', 'items');
        return collect($response->json('Items', []));
    }

    private function fetchTrackingCategories(XeroOrganisation $organisation, string $label): Collection
    {
        $response = $this->getWithRetry($organisation, $label, 'https://api.xero.com/api.xro/2.0/TrackingCategories', 'tracking categories');
        return collect($response->json('TrackingCategories', []));
    }

    private function getWithRetry(
        XeroOrganisation $organisation,
        string $label,
        string $url,
        string $resourceLabel
    ): Response {
        $maxAttempts = 6;
        $attempt = 1;

        while ($attempt <= $maxAttempts) {
            $response = Http::withToken($organisation->token->access_token)
                ->withHeaders([
                    'Xero-tenant-id' => $this->getTenantId($organisation),
                    'Accept' => 'application/json',
                ])
                ->get($url);

            if ($response->successful()) {
                return $response;
            }

            if ($response->status() === 429) {
                $retryAfter = (int) ($response->header('Retry-After') ?? 5);
                $retryAfter = $retryAfter > 0 ? $retryAfter : 5;

                $this->warn("Rate limited while fetching {$label} {$resourceLabel}. Waiting {$retryAfter} seconds before retry {$attempt}/{$maxAttempts}...");
                sleep($retryAfter);
                $attempt++;
                continue;
            }

            $this->error("Failed to fetch {$label} {$resourceLabel}.");
            $this->line('HTTP Status: ' . $response->status());
            $this->line($response->body());
            throw new \RuntimeException("Failed to fetch {$label} {$resourceLabel}.");
        }

        throw new \RuntimeException("Failed to fetch {$label} {$resourceLabel} after retries.");
    }

    private function postRepeatingInvoicesWithRetry(
        XeroOrganisation $organisation,
        array $payload,
        string $contactName,
        string $sourceId
    ): Response {
        $maxAttempts = 6;
        $attempt = 1;

        while ($attempt <= $maxAttempts) {
            $response = Http::withToken($organisation->token->access_token)
                ->withHeaders([
                    'Xero-tenant-id' => $this->getTenantId($organisation),
                    'Accept' => 'application/json',
                    'Content-Type' => 'application/json',
                ])
                ->post('https://api.xero.com/api.xro/2.0/RepeatingInvoices', $payload);

            if ($response->successful()) {
                return $response;
            }

            if ($response->status() === 429) {
                $retryAfter = (int) ($response->header('Retry-After') ?? 5);
                $retryAfter = $retryAfter > 0 ? $retryAfter : 5;

                $this->warn("Rate limited while creating repeating invoice {$contactName} ({$sourceId}). Waiting {$retryAfter} seconds before retry {$attempt}/{$maxAttempts}...");
                sleep($retryAfter);
                $attempt++;
                continue;
            }

            return $response;
        }

        throw new \RuntimeException("Rate limited while creating repeating invoice {$contactName} ({$sourceId}) after retries.");
    }

    private function buildDestinationTaxTypeSet(Collection $taxRates): array
    {
        $set = [];

        foreach ($taxRates as $taxRate) {
            $taxType = trim((string) ($taxRate['TaxType'] ?? ''));
            if ($taxType !== '') {
                $set[$taxType] = true;
            }
        }

        return $set;
    }

    private function buildDestinationTrackingIndexes(Collection $trackingCategories): array
    {
        $byCategoryId = collect();
        $byOptionId = collect();

        foreach ($trackingCategories as $category) {
            $categoryId = $category['TrackingCategoryID'] ?? null;
            if (!$categoryId) {
                continue;
            }

            $byCategoryId->put($categoryId, $category);

            foreach (($category['Options'] ?? []) as $option) {
                $optionId = $option['TrackingOptionID'] ?? null;
                if (!$optionId) {
                    continue;
                }

                $option['_parent_category_id'] = $categoryId;
                $option['_parent_category_name'] = $category['Name'] ?? null;
                $byOptionId->put($optionId, $option);
            }
        }

        return [$byCategoryId, $byOptionId];
    }

    private function getTenantId(XeroOrganisation $organisation): ?string
    {
        return $organisation->xero_tenant_id
            ?? $organisation->tenant_id
            ?? null;
    }

    private function parseXeroDate(?string $value): ?Carbon
    {
        if (!$value) {
            return null;
        }

        try {
            return Carbon::parse($value)->startOfDay();
        } catch (\Throwable $e) {
            if (preg_match('/\/Date\((\d+)/', $value, $matches)) {
                return Carbon::createFromTimestampMs((int) $matches[1])->startOfDay();
            }

            return null;
        }
    }

    private function formatXeroDateToYmd(?string $value): ?string
    {
        $date = $this->parseXeroDate($value);
        return $date ? $date->format('Y-m-d') : null;
    }

    private function normalizeTextValue(?string $value): string
    {
        if (!$value) {
            return '';
        }

        $value = str_replace(["\r\n", "\r"], "\n", $value);
        $value = preg_replace('/\s+/', ' ', trim($value));

        return $value ?? '';
    }

    private function normalizeNumericValue($value): string
    {
        if ($value === null || $value === '') {
            return '';
        }

        return number_format((float) $value, 4, '.', '');
    }

    private function normalizeName(?string $value): string
    {
        if (!$value) {
            return '';
        }

        return strtolower(preg_replace('/[^a-z0-9]/i', '', $value));
    }

    private function normalizeDiscountFields(array $cleanLine): array
    {
        $hasDiscountRate = array_key_exists('DiscountRate', $cleanLine)
            && $cleanLine['DiscountRate'] !== null
            && $cleanLine['DiscountRate'] !== '';

        if (!$hasDiscountRate) {
            unset($cleanLine['DiscountEnteredAsPercent']);
            return $cleanLine;
        }

        $cleanLine['DiscountRate'] = (float) $cleanLine['DiscountRate'];
        $cleanLine['DiscountEnteredAsPercent'] = true;

        if (
            isset($cleanLine['Quantity'], $cleanLine['UnitAmount'], $cleanLine['LineAmount']) &&
            (float) $cleanLine['DiscountRate'] >= 99.9999
        ) {
            $expectedLineAmount = round(
                (float) $cleanLine['Quantity'] * (float) $cleanLine['UnitAmount'] * (1 - ((float) $cleanLine['DiscountRate'] / 100)),
                2
            );

            $cleanLine['LineAmount'] = $expectedLineAmount;
        }

        return $cleanLine;
    }

    private function selectBatchInvoices(
        Collection $sourceRepeatingInvoices,
        int $batchSize,
        string $destinationRole,
        XeroIdMapper $mapper,
        XeroOrganisation $source,
        XeroOrganisation $destination,
        Collection $destinationAccountsByCode,
        array $destinationTaxTypes,
        Collection $destinationItemsByCode,
        Collection $destinationTrackingByCategoryId,
        Collection $destinationTrackingByOptionId,
        Collection $destinationRepeatingById
    ): Collection {
        if ($batchSize <= 0) {
            return $sourceRepeatingInvoices->values();
        }

        $selected = collect();

        foreach ($sourceRepeatingInvoices as $sourceInvoice) {
            if ($selected->count() >= $batchSize) {
                break;
            }

            if (!$this->isAlreadyValidlyMigrated(
                $sourceInvoice,
                $destinationRole,
                $mapper,
                $source,
                $destination,
                $destinationAccountsByCode,
                $destinationTaxTypes,
                $destinationItemsByCode,
                $destinationTrackingByCategoryId,
                $destinationTrackingByOptionId,
                $destinationRepeatingById
            )) {
                $selected->push($sourceInvoice);
            }
        }

        return $selected->values();
    }

    private function isAlreadyValidlyMigrated(
        array $sourceInvoice,
        string $destinationRole,
        XeroIdMapper $mapper,
        XeroOrganisation $source,
        XeroOrganisation $destination,
        Collection $destinationAccountsByCode,
        array $destinationTaxTypes,
        Collection $destinationItemsByCode,
        Collection $destinationTrackingByCategoryId,
        Collection $destinationTrackingByOptionId,
        Collection $destinationRepeatingById
    ): bool {
        $sourceId = $sourceInvoice['RepeatingInvoiceID'] ?? $sourceInvoice['ID'] ?? null;

        if (!$sourceId) {
            return false;
        }

        $payloadResult = $this->buildPayloadForDestination(
            $sourceInvoice,
            $destinationRole,
            $mapper,
            $source,
            $destination,
            $destinationAccountsByCode,
            $destinationTaxTypes,
            $destinationItemsByCode,
            $destinationTrackingByCategoryId,
            $destinationTrackingByOptionId
        );

        if (!$payloadResult['ok']) {
            return false;
        }

        $payloadInvoice = $payloadResult['payload']['RepeatingInvoices'][0];
        $fingerprint = $this->buildPayloadFingerprint($payloadInvoice);

        $mappedId = $destinationRole === 'test'
            ? $mapper->getTestId(self::ENTITY, $sourceId)
            : $mapper->getTargetId(self::ENTITY, $sourceId);

        if (!$mappedId) {
            return false;
        }

        $mappedInvoice = $destinationRepeatingById->get($mappedId);

        if (!$mappedInvoice) {
            return false;
        }

        $destinationFingerprint = $this->buildDestinationFingerprint($mappedInvoice, $destinationTrackingByOptionId);

        return $destinationFingerprint === $fingerprint;
    }
    private function storeMapping(
        XeroIdMapper $mapper,
        string $destinationRole,
        XeroOrganisation $source,
        XeroOrganisation $destination,
        string $sourceId,
        string $destinationId,
        string $name
    ): void {
        if ($destinationRole === 'test') {
            $mapper->storeTest(
                self::ENTITY,
                $sourceId,
                $destinationId,
                $this->getTenantId($source),
                $this->getTenantId($destination),
                $name
            );
            return;
        }

        $mapper->storeTarget(
            self::ENTITY,
            $sourceId,
            $destinationId,
            $this->getTenantId($source),
            $this->getTenantId($destination),
            $name
        );
    }
    private function explainFingerprintMismatch(
        array $payloadInvoice,
        array $mappedInvoice,
        Collection $destinationTrackingByOptionId
    ): string {
        $source = $this->buildPayloadFingerprintData($payloadInvoice);
        $target = $this->buildDestinationFingerprintData($mappedInvoice, $destinationTrackingByOptionId);

        if (($source['Status'] ?? null) !== ($target['Status'] ?? null)) {
            return "status mismatch ({$source['Status']} vs {$target['Status']})";
        }

        if (($source['LineAmountTypes'] ?? null) !== ($target['LineAmountTypes'] ?? null)) {
            return "line amount type mismatch ({$source['LineAmountTypes']} vs {$target['LineAmountTypes']})";
        }

        if (($source['Reference'] ?? null) !== ($target['Reference'] ?? null)) {
            return "reference mismatch";
        }

        if (($source['CurrencyCode'] ?? null) !== ($target['CurrencyCode'] ?? null)) {
            return "currency mismatch ({$source['CurrencyCode']} vs {$target['CurrencyCode']})";
        }

        if (($source['BrandingThemeID'] ?? null) !== ($target['BrandingThemeID'] ?? null)) {
            return "branding theme mismatch";
        }

        if (($source['Schedule']['Period'] ?? null) !== ($target['Schedule']['Period'] ?? null)) {
            return "schedule period mismatch";
        }

        if (($source['Schedule']['Unit'] ?? null) !== ($target['Schedule']['Unit'] ?? null)) {
            return "schedule unit mismatch";
        }

        if (($source['Schedule']['DueDate'] ?? null) !== ($target['Schedule']['DueDate'] ?? null)) {
            return "schedule due date mismatch";
        }

        if (($source['Schedule']['DueDateType'] ?? null) !== ($target['Schedule']['DueDateType'] ?? null)) {
            return "schedule due date type mismatch";
        }

        if (($source['Schedule']['StartDate'] ?? null) !== ($target['Schedule']['StartDate'] ?? null)) {
            return "schedule start date mismatch";
        }

        if (($source['Schedule']['EndDate'] ?? null) !== ($target['Schedule']['EndDate'] ?? null)) {
            return "schedule end date mismatch";
        }

        $sourceLines = $source['LineItems'] ?? [];
        $targetLines = $target['LineItems'] ?? [];

        if (count($sourceLines) !== count($targetLines)) {
            return 'line count mismatch';
        }

        foreach ($sourceLines as $i => $sourceLine) {
            $targetLine = $targetLines[$i] ?? null;

            if (!$targetLine) {
                return 'line structure mismatch';
            }

            if (($sourceLine['Description'] ?? null) !== ($targetLine['Description'] ?? null)) {
                return 'line ' . ($i + 1) . ' description mismatch';
            }

            if (($sourceLine['Quantity'] ?? null) !== ($targetLine['Quantity'] ?? null)) {
                return 'line ' . ($i + 1) . ' quantity mismatch';
            }

            if (($sourceLine['UnitAmount'] ?? null) !== ($targetLine['UnitAmount'] ?? null)) {
                return 'line ' . ($i + 1) . ' unit amount mismatch';
            }

            if (($sourceLine['LineAmount'] ?? null) !== ($targetLine['LineAmount'] ?? null)) {
                return 'line ' . ($i + 1) . ' line amount mismatch';
            }

            if (($sourceLine['AccountCode'] ?? null) !== ($targetLine['AccountCode'] ?? null)) {
                return 'line ' . ($i + 1) . ' account code mismatch';
            }

            if (($sourceLine['TaxType'] ?? null) !== ($targetLine['TaxType'] ?? null)) {
                return 'line ' . ($i + 1) . ' tax type mismatch';
            }

            if (($sourceLine['ItemCode'] ?? null) !== ($targetLine['ItemCode'] ?? null)) {
                return 'line ' . ($i + 1) . ' item code mismatch';
            }

            if (($sourceLine['DiscountRate'] ?? null) !== ($targetLine['DiscountRate'] ?? null)) {
                return 'line ' . ($i + 1) . ' discount rate mismatch';
            }

            if (($sourceLine['DiscountEnteredAsPercent'] ?? null) !== ($targetLine['DiscountEnteredAsPercent'] ?? null)) {
                return 'line ' . ($i + 1) . ' discount mode mismatch';
            }

            if (($sourceLine['Tracking'] ?? []) !== ($targetLine['Tracking'] ?? [])) {
                return 'line ' . ($i + 1) . ' tracking mismatch';
            }
        }

        return 'unknown fingerprint mismatch';
    }
}
