<?php

namespace App\Console\Commands\Contacts;

use App\Models\XeroOrganisation;
use Carbon\Carbon;
use Illuminate\Console\Command;
use Illuminate\Support\Facades\DB;
use Illuminate\Support\Facades\Http;
use Throwable;

class XeroValidateContactPairs extends Command
{
    protected $signature = 'xero:contacts:validate-pairs
                            {--source=source : Source organisation role}
                            {--target=target : Target organisation role}
                            {--table=xero_repeating_invoice_checks : Read-only source table containing contact IDs}
                            {--output-table=xero_repeating_invoice_contact_checks : Output table for contact check results}
                            {--id= : Process one specific row ID from xero_repeating_invoice_contact_checks}
                            {--first : Process only the first available row}
                            {--random : Process only one random available row}
                            {--limit= : Process only the next N available rows}
                            {--batch=50 : Batch size for normal runs}
                            {--wait=65 : Seconds to wait between batches}
                            {--dry-run : Do not write anything, only analyse and print}';

    protected $description = 'Validate contact pairs from xero_repeating_invoice_checks and write results to xero_repeating_invoice_contact_checks';

    public function handle(): int
    {
        $sourceRole = (string) $this->option('source');
        $targetRole = (string) $this->option('target');
        $table = (string) $this->option('table');
        $outputTable = (string) $this->option('output-table');
        $specificId = (int) ($this->option('id') ?? 0);
        $useFirst = (bool) $this->option('first');
        $useRandom = (bool) $this->option('random');
        $dryRun = (bool) $this->option('dry-run');
        $limit = max(0, (int) ($this->option('limit') ?? 0));
        $batchSize = max(1, (int) ($this->option('batch') ?? 50));
        $waitSeconds = max(0, (int) ($this->option('wait') ?? 65));

        if ($useFirst && $useRandom) {
            $this->error('Use only one of --first or --random, not both.');
            return self::FAILURE;
        }

        if ($specificId > 0 && ($useFirst || $useRandom || $limit > 0)) {
            $this->error('Do not combine --id with --first, --random, or --limit.');
            return self::FAILURE;
        }

        $this->info('Starting contact pair validation...');
        $this->line('Source role: ' . $sourceRole);
        $this->line('Target role: ' . $targetRole);
        $this->line('Input table: ' . $table);
        $this->line('Output table: ' . $outputTable);
        $this->line('Mode: ' . ($dryRun ? 'DRY RUN' : 'LIVE WRITE'));

        if ($specificId > 0) {
            $this->line('Selection mode: SPECIFIC ID = ' . $specificId);
        } elseif ($useFirst) {
            $this->line('Selection mode: FIRST ROW ONLY');
        } elseif ($useRandom) {
            $this->line('Selection mode: RANDOM ROW ONLY');
        } elseif ($limit > 0) {
            $this->line('Selection mode: LIMIT = ' . $limit);
        } else {
            $this->line('Selection mode: BATCH LOOP');
            $this->line('Batch size: ' . $batchSize);
            $this->line('Wait between batches: ' . $waitSeconds . ' seconds');
        }

        $this->newLine();

        $source = XeroOrganisation::where('role', $sourceRole)->first();
        $target = XeroOrganisation::where('role', $targetRole)->first();

        if (!$source || !$target) {
            $this->error('Source or target organisation not found.');
            return self::FAILURE;
        }

        if (!$source->token || !$target->token) {
            $this->error('Source or target token not found.');
            return self::FAILURE;
        }

        $sourceTenantId = $this->getTenantId($source);
        $targetTenantId = $this->getTenantId($target);

        if (!$sourceTenantId || !$targetTenantId) {
            $this->error('Source or target tenant ID not found.');
            return self::FAILURE;
        }

        $sourceAccessToken = $source->token->access_token ?? null;
        $targetAccessToken = $target->token->access_token ?? null;

        if (!$sourceAccessToken || !$targetAccessToken) {
            $this->error('Source or target access token not found.');
            return self::FAILURE;
        }

        $processed = 0;
        $okCount = 0;
        $flaggedCount = 0;
        $errorCount = 0;

        if ($specificId > 0) {
            $row = $this->loadSpecificOutputRow($outputTable, $specificId);

            if (!$row) {
                $this->error('No row found in ' . $outputTable . ' for ID ' . $specificId);
                return self::FAILURE;
            }

            $result = $this->processRow(
                $row,
                $outputTable,
                $sourceAccessToken,
                $sourceTenantId,
                $targetAccessToken,
                $targetTenantId,
                $dryRun
            );

            $processed = 1;
            $okCount = $result['ok'];
            $flaggedCount = $result['flagged'];
            $errorCount = $result['error'];
        } elseif ($useFirst || $useRandom || $limit > 0) {
            $rows = $this->loadAvailableRowsFromTables($table, $outputTable);

            if (empty($rows)) {
                $this->warn('No available rows found to validate.');
                return self::SUCCESS;
            }

            if ($useFirst) {
                $rows = [array_values($rows)[0]];
            } elseif ($useRandom) {
                $randomKey = array_rand($rows);
                $rows = [$rows[$randomKey]];
            } elseif ($limit > 0) {
                $rows = array_slice(array_values($rows), 0, $limit);
            }

            $this->line('Rows selected: ' . count($rows));

            foreach ($rows as $row) {
                $result = $this->processRow(
                    $row,
                    $outputTable,
                    $sourceAccessToken,
                    $sourceTenantId,
                    $targetAccessToken,
                    $targetTenantId,
                    $dryRun
                );

                $processed++;
                $okCount += $result['ok'];
                $flaggedCount += $result['flagged'];
                $errorCount += $result['error'];
            }
        } else {
            $batchNumber = 0;

            while (true) {
                $rows = $this->loadAvailableRowsFromTables($table, $outputTable, $batchSize);

                if (empty($rows)) {
                    if ($batchNumber === 0) {
                        $this->warn('No available rows found to validate.');
                    } else {
                        $this->info('No more available rows remaining.');
                    }
                    break;
                }

                $batchNumber++;
                $this->newLine();
                $this->info('Batch ' . $batchNumber . ' -> rows selected: ' . count($rows));

                foreach ($rows as $row) {
                    $result = $this->processRow(
                        $row,
                        $outputTable,
                        $sourceAccessToken,
                        $sourceTenantId,
                        $targetAccessToken,
                        $targetTenantId,
                        $dryRun
                    );

                    $processed++;
                    $okCount += $result['ok'];
                    $flaggedCount += $result['flagged'];
                    $errorCount += $result['error'];
                }

                $remainingRows = $this->countAvailableRowsFromTables($table, $outputTable);

                $this->newLine();
                $this->line('Running summary');
                $this->line('---------------');
                $this->line('Processed so far: ' . $processed);
                $this->line('OK so far: ' . $okCount);
                $this->line('FLAGGED so far: ' . $flaggedCount);
                $this->line('ERROR so far: ' . $errorCount);
                $this->line('Remaining rows: ' . $remainingRows);

                if ($remainingRows <= 0) {
                    $this->info('All available rows are done.');
                    break;
                }

                if (!$this->confirm('Continue to next batch?', true)) {
                    $this->warn('Aborted by user.');
                    break;
                }

                if ($waitSeconds > 0) {
                    $this->line('Waiting ' . $waitSeconds . ' seconds before next batch...');
                    sleep($waitSeconds);
                }
            }
        }

        $this->newLine();
        $this->info('Validation complete.');
        $this->line('Summary');
        $this->line('-------');
        $this->line('Processed: ' . $processed);
        $this->line('OK: ' . $okCount);
        $this->line('FLAGGED: ' . $flaggedCount);
        $this->line('ERROR: ' . $errorCount);

        return self::SUCCESS;
    }

    protected function processRow(
        array $row,
        string $outputTable,
        string $sourceAccessToken,
        string $sourceTenantId,
        string $targetAccessToken,
        string $targetTenantId,
        bool $dryRun
    ): array {
        $rowId = (int) ($row['id'] ?? 0);
        $sourceContactId = trim((string) ($row['source_contact_id'] ?? ''));
        $targetContactId = trim((string) ($row['target_contact_id'] ?? ''));

        $this->newLine();
        $this->info('Row ID: ' . $rowId);

        if ($sourceContactId === '' || $targetContactId === '') {
            $message = 'Missing source_contact_id or target_contact_id';
            $this->error('Result: ' . $message);

            if (!$dryRun) {
                $this->saveResult(
                    $outputTable,
                    $rowId,
                    $sourceContactId,
                    $targetContactId,
                    null,
                    null,
                    'ERROR',
                    $message
                );
            }

            return ['ok' => 0, 'flagged' => 0, 'error' => 1];
        }

        $this->line('Fetching source contact: ' . $sourceContactId);
        $this->line('Fetching target contact: ' . $targetContactId);

        try {
            $sourceContact = $this->fetchContact(
                $sourceAccessToken,
                $sourceTenantId,
                $sourceContactId,
                'SOURCE'
            );

            $targetContact = $this->fetchContact(
                $targetAccessToken,
                $targetTenantId,
                $targetContactId,
                'TARGET'
            );

            if (!$sourceContact) {
                $message = 'Source contact not found';
                $this->error('Result: ' . $message);

                if (!$dryRun) {
                    $this->saveResult(
                        $outputTable,
                        $rowId,
                        $sourceContactId,
                        $targetContactId,
                        null,
                        null,
                        'ERROR',
                        $message
                    );
                }

                return ['ok' => 0, 'flagged' => 0, 'error' => 1];
            }

            if (!$targetContact) {
                $message = 'Target contact not found';
                $this->error('Result: ' . $message);

                if (!$dryRun) {
                    $this->saveResult(
                        $outputTable,
                        $rowId,
                        $sourceContactId,
                        $targetContactId,
                        trim((string) data_get($sourceContact, 'Name', '')),
                        null,
                        'ERROR',
                        $message
                    );
                }

                return ['ok' => 0, 'flagged' => 0, 'error' => 1];
            }

            $sourceName = trim((string) data_get($sourceContact, 'Name', ''));
            $targetName = trim((string) data_get($targetContact, 'Name', ''));

            $sourceEmail = trim((string) data_get($sourceContact, 'EmailAddress', ''));
            $targetEmail = trim((string) data_get($targetContact, 'EmailAddress', ''));

            $sourceAccountNumber = trim((string) data_get($sourceContact, 'AccountNumber', ''));
            $targetAccountNumber = trim((string) data_get($targetContact, 'AccountNumber', ''));

            $sourceContactStatus = trim((string) data_get($sourceContact, 'ContactStatus', ''));
            $targetContactStatus = trim((string) data_get($targetContact, 'ContactStatus', ''));

            $sourceFirstName = trim((string) data_get($sourceContact, 'FirstName', ''));
            $targetFirstName = trim((string) data_get($targetContact, 'FirstName', ''));

            $sourceLastName = trim((string) data_get($sourceContact, 'LastName', ''));
            $targetLastName = trim((string) data_get($targetContact, 'LastName', ''));

            $sourceTaxNumber = trim((string) data_get($sourceContact, 'TaxNumber', ''));
            $targetTaxNumber = trim((string) data_get($targetContact, 'TaxNumber', ''));

            $sourcePhones = $this->normalizePhones(data_get($sourceContact, 'Phones', []));
            $targetPhones = $this->normalizePhones(data_get($targetContact, 'Phones', []));

            $sourceAddresses = $this->normalizeAddresses(data_get($sourceContact, 'Addresses', []));
            $targetAddresses = $this->normalizeAddresses(data_get($targetContact, 'Addresses', []));

            $issues = [];

            if ($sourceName !== $targetName) {
                $issues[] = 'Name is not matching';
            }

            if ($sourceEmail !== $targetEmail) {
                $issues[] = 'EmailAddress is not matching';
            }

            if ($sourceAccountNumber !== $targetAccountNumber) {
                $issues[] = 'AccountNumber is not matching';
            }

            if ($sourceContactStatus !== $targetContactStatus) {
                $issues[] = 'ContactStatus is not matching';
            }

            if ($sourceFirstName !== $targetFirstName) {
                $issues[] = 'FirstName is not matching';
            }

            if ($sourceLastName !== $targetLastName) {
                $issues[] = 'LastName is not matching';
            }

            if ($sourceTaxNumber !== $targetTaxNumber) {
                $issues[] = 'TaxNumber is not matching';
            }

            if ($sourcePhones !== $targetPhones) {
                $issues[] = 'Phones are not matching';
            }

            if ($sourceAddresses !== $targetAddresses) {
                $issues[] = 'Addresses are not matching';
            }

            $this->line('Source Contact ID: ' . $sourceContactId);
            $this->line('Target Contact ID: ' . $targetContactId);
            $this->line('Source Name: ' . ($sourceName !== '' ? $sourceName : 'NULL'));
            $this->line('Target Name: ' . ($targetName !== '' ? $targetName : 'NULL'));
            $this->line('Source EmailAddress: ' . ($sourceEmail !== '' ? $sourceEmail : 'NULL'));
            $this->line('Target EmailAddress: ' . ($targetEmail !== '' ? $targetEmail : 'NULL'));
            $this->line('Source AccountNumber: ' . ($sourceAccountNumber !== '' ? $sourceAccountNumber : 'NULL'));
            $this->line('Target AccountNumber: ' . ($targetAccountNumber !== '' ? $targetAccountNumber : 'NULL'));
            $this->line('Source ContactStatus: ' . ($sourceContactStatus !== '' ? $sourceContactStatus : 'NULL'));
            $this->line('Target ContactStatus: ' . ($targetContactStatus !== '' ? $targetContactStatus : 'NULL'));
            $this->line('Source FirstName: ' . ($sourceFirstName !== '' ? $sourceFirstName : 'NULL'));
            $this->line('Target FirstName: ' . ($targetFirstName !== '' ? $targetFirstName : 'NULL'));
            $this->line('Source LastName: ' . ($sourceLastName !== '' ? $sourceLastName : 'NULL'));
            $this->line('Target LastName: ' . ($targetLastName !== '' ? $targetLastName : 'NULL'));
            $this->line('Source TaxNumber: ' . ($sourceTaxNumber !== '' ? $sourceTaxNumber : 'NULL'));
            $this->line('Target TaxNumber: ' . ($targetTaxNumber !== '' ? $targetTaxNumber : 'NULL'));
            $this->line('Source Phones: ' . $this->formatNormalizedList($sourcePhones));
            $this->line('Target Phones: ' . $this->formatNormalizedList($targetPhones));
            $this->line('Source Addresses: ' . $this->formatNormalizedList($sourceAddresses));
            $this->line('Target Addresses: ' . $this->formatNormalizedList($targetAddresses));

            if (empty($issues)) {
                $message = '';
                $this->info('Result: OK');

                if (!$dryRun) {
                    $this->saveResult(
                        $outputTable,
                        $rowId,
                        $sourceContactId,
                        $targetContactId,
                        $sourceName,
                        $targetName,
                        'OK',
                        $message
                    );
                }

                $this->line(str_repeat('-', 100));
                return ['ok' => 1, 'flagged' => 0, 'error' => 0];
            }

            $message = implode('; ', $issues);
            $this->warn('Result: ' . $message);

            if (!$dryRun) {
                $this->saveResult(
                    $outputTable,
                    $rowId,
                    $sourceContactId,
                    $targetContactId,
                    $sourceName,
                    $targetName,
                    'FLAGGED',
                    $message
                );
            }

            $this->line(str_repeat('-', 100));
            return ['ok' => 0, 'flagged' => 1, 'error' => 0];
        } catch (Throwable $e) {
            $message = $e->getMessage();
            $this->error('Result: ERROR - ' . $message);

            if (!$dryRun) {
                $this->saveResult(
                    $outputTable,
                    $rowId,
                    $sourceContactId,
                    $targetContactId,
                    null,
                    null,
                    'ERROR',
                    $message
                );
            }

            $this->line(str_repeat('-', 100));
            return ['ok' => 0, 'flagged' => 0, 'error' => 1];
        }
    }

    protected function loadAvailableRowsFromTables(string $table, string $outputTable, ?int $limit = null): array
    {
        $query = DB::table($table . ' as ric')
            ->leftJoin($outputTable . ' as ricc', 'ric.id', '=', 'ricc.xero_repeating_invoice_check_id')
            ->select([
                'ric.id',
                'ric.source_contact_id',
                'ric.target_contact_id',
            ])
            ->whereNotNull('ric.source_contact_id')
            ->whereNotNull('ric.target_contact_id')
            ->where('ric.source_contact_id', '<>', '')
            ->where('ric.target_contact_id', '<>', '')
            ->where(function ($q) {
                $q->whereNull('ricc.id')
                    ->orWhereNull('ricc.checked_at');
            })
            ->orderBy('ric.id');

        if ($limit !== null && $limit > 0) {
            $query->limit($limit);
        }

        $rows = $query->get();

        return $rows->map(function ($row) {
            return [
                'id' => (int) $row->id,
                'source_contact_id' => trim((string) ($row->source_contact_id ?? '')),
                'target_contact_id' => trim((string) ($row->target_contact_id ?? '')),
            ];
        })->values()->all();
    }

    protected function loadSpecificOutputRow(string $outputTable, int $id): ?array
    {
        $row = DB::table($outputTable)
            ->select([
                'id',
                'xero_repeating_invoice_check_id',
                'source_contact_id',
                'target_contact_id',
            ])
            ->where('id', $id)
            ->first();

        if (!$row) {
            return null;
        }

        return [
            'id' => (int) ($row->xero_repeating_invoice_check_id ?? 0),
            'source_contact_id' => trim((string) ($row->source_contact_id ?? '')),
            'target_contact_id' => trim((string) ($row->target_contact_id ?? '')),
        ];
    }

    protected function countAvailableRowsFromTables(string $table, string $outputTable): int
    {
        return (int) DB::table($table . ' as ric')
            ->leftJoin($outputTable . ' as ricc', 'ric.id', '=', 'ricc.xero_repeating_invoice_check_id')
            ->whereNotNull('ric.source_contact_id')
            ->whereNotNull('ric.target_contact_id')
            ->where('ric.source_contact_id', '<>', '')
            ->where('ric.target_contact_id', '<>', '')
            ->where(function ($q) {
                $q->whereNull('ricc.id')
                    ->orWhereNull('ricc.checked_at');
            })
            ->count();
    }

    protected function saveResult(
        string $outputTable,
        int $xeroRepeatingInvoiceCheckId,
        string $sourceContactId,
        string $targetContactId,
        ?string $sourceName,
        ?string $targetName,
        string $result,
        string $message
    ): void {
        $existing = DB::table($outputTable)
            ->where('xero_repeating_invoice_check_id', $xeroRepeatingInvoiceCheckId)
            ->first();

        $now = Carbon::now();

        $payload = [
            'source_contact_id' => $sourceContactId !== '' ? $sourceContactId : null,
            'target_contact_id' => $targetContactId !== '' ? $targetContactId : null,
            'source_name' => ($sourceName !== null && $sourceName !== '') ? $sourceName : null,
            'target_name' => ($targetName !== null && $targetName !== '') ? $targetName : null,
            'result' => $result,
            'message' => $message !== '' ? $message : null,
            'checked_at' => $now,
            'updated_at' => $now,
        ];

        if ($existing) {
            DB::table($outputTable)
                ->where('xero_repeating_invoice_check_id', $xeroRepeatingInvoiceCheckId)
                ->update($payload);

            return;
        }

        $payload['xero_repeating_invoice_check_id'] = $xeroRepeatingInvoiceCheckId;
        $payload['created_at'] = $now;

        DB::table($outputTable)->insert($payload);
    }

    protected function fetchContact(
        string $accessToken,
        string $tenantId,
        string $contactId,
        string $label
    ): ?array {
        $url = 'https://api.xero.com/api.xro/2.0/Contacts/' . $contactId;
        $maxAttempts = 6;
        $attempt = 1;

        while ($attempt <= $maxAttempts) {
            $response = Http::withToken($accessToken)
                ->withHeaders([
                    'Xero-tenant-id' => $tenantId,
                    'Accept' => 'application/json',
                ])
                ->timeout(60)
                ->get($url);

            if ($response->successful()) {
                $data = $response->json();

                if (!isset($data['Contacts']) || !is_array($data['Contacts']) || empty($data['Contacts'])) {
                    return null;
                }

                return $data['Contacts'][0];
            }

            if ($response->status() === 429) {
                $retryAfter = (int) ($response->header('Retry-After') ?? 5);
                $retryAfter = $retryAfter > 0 ? $retryAfter : 5;

                $this->warn("Rate limited while fetching {$label} contact {$contactId}. Waiting {$retryAfter} seconds before retry {$attempt}/{$maxAttempts}...");
                sleep($retryAfter);
                $attempt++;
                continue;
            }

            throw new \RuntimeException(
                'Xero API request failed for ' . $label . ' contact [' . $contactId . '] with status '
                . $response->status() . ': ' . $response->body()
            );
        }

        throw new \RuntimeException(
            'Rate limited while fetching ' . $label . ' contact [' . $contactId . '] after retries.'
        );
    }

    protected function normalizePhones($phones): array
    {
        if (!is_array($phones)) {
            return [];
        }

        $normalized = [];

        foreach ($phones as $phone) {
            $normalized[] = implode('|', [
                trim((string) data_get($phone, 'PhoneType', '')),
                trim((string) data_get($phone, 'PhoneCountryCode', '')),
                trim((string) data_get($phone, 'PhoneAreaCode', '')),
                trim((string) data_get($phone, 'PhoneNumber', '')),
            ]);
        }

        $normalized = array_filter($normalized, fn ($value) => trim((string) $value, '|') !== '');
        sort($normalized);

        return array_values($normalized);
    }

    protected function normalizeAddresses($addresses): array
    {
        if (!is_array($addresses)) {
            return [];
        }

        $normalized = [];

        foreach ($addresses as $address) {
            $normalized[] = implode('|', [
                trim((string) data_get($address, 'AddressType', '')),
                trim((string) data_get($address, 'AddressLine1', '')),
                trim((string) data_get($address, 'AddressLine2', '')),
                trim((string) data_get($address, 'AddressLine3', '')),
                trim((string) data_get($address, 'AddressLine4', '')),
                trim((string) data_get($address, 'City', '')),
                trim((string) data_get($address, 'Region', '')),
                trim((string) data_get($address, 'PostalCode', '')),
                trim((string) data_get($address, 'Country', '')),
            ]);
        }

        $normalized = array_filter($normalized, fn ($value) => trim((string) $value, '|') !== '');
        sort($normalized);

        return array_values($normalized);
    }

    protected function formatNormalizedList(array $items): string
    {
        if (empty($items)) {
            return '(none)';
        }

        return implode(' || ', $items);
    }

    private function getTenantId(XeroOrganisation $organisation): ?string
    {
        return $organisation->xero_tenant_id
            ?? $organisation->tenant_id
            ?? null;
    }
}