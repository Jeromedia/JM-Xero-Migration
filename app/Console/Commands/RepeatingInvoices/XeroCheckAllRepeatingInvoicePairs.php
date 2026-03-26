<?php

namespace App\Console\Commands\RepeatingInvoices;

use App\Models\XeroOrganisation;
use App\Models\XeroRepeatingInvoiceCheck;
use Illuminate\Console\Command;
use Illuminate\Support\Facades\DB;
use Illuminate\Support\Facades\Http;
use RuntimeException;
use Throwable;

class XeroCheckAllRepeatingInvoicePairs extends Command
{
    protected $signature = 'xero:check-all-repeating-invoice-pairs
                            {--source=source : Source organisation role}
                            {--target=target : Target organisation role}
                            {--table=mapping_xero_repeating_invoice : Mapping table name}
                            {--check-table=xero_repeating_invoice_checks : Check table name}
                            {--first : Process only the first unchecked mapping row}
                            {--random : Process only one random unchecked mapping row}
                            {--dry-run : Test DB load and Xero connection with one unchecked row only}
                            {--batch-rows=50 : Number of rows to process before pausing}
                            {--pause-seconds=65 : Pause duration after each batch of rows}';

    protected $description = 'Check repeating invoice pairs from mapping table, save results, and resume by checked_at';

    public function handle(): int
    {
        $sourceRole = (string) $this->option('source');
        $targetRole = (string) $this->option('target');
        $tableName = trim((string) $this->option('table'));
        $checkTableName = trim((string) $this->option('check-table'));
        $useFirst = (bool) $this->option('first');
        $useRandom = (bool) $this->option('random');
        $dryRun = (bool) $this->option('dry-run');
        $batchRows = max(1, (int) $this->option('batch-rows'));
        $pauseSeconds = max(1, (int) $this->option('pause-seconds'));

        if ($useFirst && $useRandom) {
            $this->error('Use only one of --first or --random, not both.');
            return self::FAILURE;
        }

        $this->info('Starting repeating invoice pair check...');
        $this->line('Source role: ' . $sourceRole);
        $this->line('Target role: ' . $targetRole);
        $this->line('Mapping table: ' . $tableName);
        $this->line('Check table: ' . $checkTableName);
        $this->line('Batch rows before pause: ' . $batchRows);
        $this->line('Pause seconds: ' . $pauseSeconds);

        if ($dryRun) {
            $this->line('Mode: DRY RUN');
        } elseif ($useFirst) {
            $this->line('Mode: FIRST UNCHECKED ROW ONLY');
        } elseif ($useRandom) {
            $this->line('Mode: RANDOM UNCHECKED ROW ONLY');
        } else {
            $this->line('Mode: ALL UNCHECKED ROWS');
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

        $this->assertTables($tableName, $checkTableName);

        $totalMappings = $this->countTotalMappings($tableName);
        $completedBeforeStart = $this->countCompletedChecks($checkTableName);
        $rows = $this->loadUncheckedRowsFromMappingTable($tableName, $checkTableName);

        if (empty($rows)) {
            $this->info('No unchecked rows found.');
            $this->line('Overall progress: ' . $completedBeforeStart . ' / ' . $totalMappings);
            return self::SUCCESS;
        }

        if ($dryRun) {
            $rows = [array_values($rows)[0]];
        } elseif ($useFirst) {
            $rows = [array_values($rows)[0]];
        } elseif ($useRandom) {
            $randomKey = array_rand($rows);
            $rows = [$rows[$randomKey]];
        }

        $totalRowsThisRun = count($rows);
        $estimatedApiCalls = $totalRowsThisRun * 2;

        $this->line('Total mappings: ' . $totalMappings);
        $this->line('Completed before start: ' . $completedBeforeStart);
        $this->line('Remaining to process now: ' . $totalRowsThisRun);
        $this->line('Estimated API calls (without retries): ' . $estimatedApiCalls);
        $this->line('Overall progress: ' . $completedBeforeStart . ' / ' . $totalMappings);
        $this->newLine();

        if ($dryRun) {
            return $this->runDryRun(
                array_values($rows)[0],
                $sourceAccessToken,
                $sourceTenantId,
                $targetAccessToken,
                $targetTenantId
            );
        }

        $okCount = 0;
        $flaggedCount = 0;
        $errorCount = 0;
        $processedCount = 0;
        $completedThisRun = 0;
        $rowsSincePause = 0;
        $abortedByUser = false;

        foreach (array_values($rows) as $index => $row) {
            $lineNumber = (int) ($row['_original_line'] ?? ($index + 1));
            $mappingId = (int) ($row['mapping_id'] ?? 0);
            $sourceRepeatingInvoiceId = trim((string) ($row['source_id'] ?? ''));
            $targetRepeatingInvoiceId = trim((string) ($row['target_id'] ?? ''));

            $this->newLine();
            $this->info('Line ' . $lineNumber . ' | Mapping ID ' . $mappingId);

            if ($mappingId <= 0 || $sourceRepeatingInvoiceId === '' || $targetRepeatingInvoiceId === '') {
                $message = 'Missing mapping_id, source_id, or target_id in mapping table';

                $this->error('Result: ' . $message);

                $completed = $this->storeCheckRecord([
                    'mapping_id' => $mappingId,
                    'source_id' => $sourceRepeatingInvoiceId,
                    'target_id' => $targetRepeatingInvoiceId,
                    'result' => 'ERROR',
                    'message' => $message,
                    'source_contact_id' => null,
                    'target_contact_id' => null,
                    'source_contact_name' => null,
                    'target_contact_name' => null,
                    'source_total' => null,
                    'target_total' => null,
                    'source_schedule' => null,
                    'target_schedule' => null,
                    'source_account_codes' => null,
                    'target_account_codes' => null,
                    'checked_at' => now(),
                ]);

                if ($completed) {
                    $completedThisRun++;
                }

                $errorCount++;
                $processedCount++;
                $this->line(str_repeat('-', 80));
                continue;
            }

            $this->line('Fetching source repeating invoice: ' . $sourceRepeatingInvoiceId);
            $this->line('Fetching target repeating invoice: ' . $targetRepeatingInvoiceId);

            try {
                $sourceInvoice = $this->fetchRepeatingInvoice(
                    $sourceAccessToken,
                    $sourceTenantId,
                    $sourceRepeatingInvoiceId,
                    'SOURCE'
                );

                $targetInvoice = $this->fetchRepeatingInvoice(
                    $targetAccessToken,
                    $targetTenantId,
                    $targetRepeatingInvoiceId,
                    'TARGET'
                );

                if (!$sourceInvoice) {
                    $message = 'Source repeating invoice not found';

                    $this->error('Result: ' . $message);

                    $completed = $this->storeCheckRecord([
                        'mapping_id' => $mappingId,
                        'source_id' => $sourceRepeatingInvoiceId,
                        'target_id' => $targetRepeatingInvoiceId,
                        'result' => 'ERROR',
                        'message' => $message,
                        'source_contact_id' => null,
                        'target_contact_id' => null,
                        'source_contact_name' => null,
                        'target_contact_name' => null,
                        'source_total' => null,
                        'target_total' => null,
                        'source_schedule' => null,
                        'target_schedule' => null,
                        'source_account_codes' => null,
                        'target_account_codes' => null,
                        'checked_at' => now(),
                    ]);

                    if ($completed) {
                        $completedThisRun++;
                    }

                    $errorCount++;
                    $processedCount++;
                    $rowsSincePause++;

                    $this->line(str_repeat('-', 80));

                    $this->handlePauseIfNeeded(
                        $rowsSincePause,
                        $batchRows,
                        $pauseSeconds,
                        $processedCount,
                        $totalRowsThisRun,
                        $completedBeforeStart + $completedThisRun,
                        $totalMappings,
                        $abortedByUser
                    );

                    if ($abortedByUser) {
                        break;
                    }

                    continue;
                }

                if (!$targetInvoice) {
                    $message = 'Target repeating invoice not found';

                    $this->error('Result: ' . $message);

                    $completed = $this->storeCheckRecord([
                        'mapping_id' => $mappingId,
                        'source_id' => $sourceRepeatingInvoiceId,
                        'target_id' => $targetRepeatingInvoiceId,
                        'result' => 'ERROR',
                        'message' => $message,
                        'source_contact_id' => trim((string) data_get($sourceInvoice, 'Contact.ContactID', '')) ?: null,
                        'target_contact_id' => null,
                        'source_contact_name' => trim((string) data_get($sourceInvoice, 'Contact.Name', '')) ?: null,
                        'target_contact_name' => null,
                        'source_total' => $this->normalizeDecimal(data_get($sourceInvoice, 'Total')),
                        'target_total' => null,
                        'source_schedule' => $this->scheduleSnapshot(data_get($sourceInvoice, 'Schedule', [])),
                        'target_schedule' => null,
                        'source_account_codes' => $this->extractAccountCodes(data_get($sourceInvoice, 'LineItems', [])),
                        'target_account_codes' => null,
                        'checked_at' => now(),
                    ]);

                    if ($completed) {
                        $completedThisRun++;
                    }

                    $errorCount++;
                    $processedCount++;
                    $rowsSincePause++;

                    $this->line(str_repeat('-', 80));

                    $this->handlePauseIfNeeded(
                        $rowsSincePause,
                        $batchRows,
                        $pauseSeconds,
                        $processedCount,
                        $totalRowsThisRun,
                        $completedBeforeStart + $completedThisRun,
                        $totalMappings,
                        $abortedByUser
                    );

                    if ($abortedByUser) {
                        break;
                    }

                    continue;
                }

                $sourceContactId = trim((string) data_get($sourceInvoice, 'Contact.ContactID', ''));
                $targetContactId = trim((string) data_get($targetInvoice, 'Contact.ContactID', ''));
                $sourceContactName = trim((string) data_get($sourceInvoice, 'Contact.Name', ''));
                $targetContactName = trim((string) data_get($targetInvoice, 'Contact.Name', ''));
                $sourceTotal = $this->normalizeDecimal(data_get($sourceInvoice, 'Total'));
                $targetTotal = $this->normalizeDecimal(data_get($targetInvoice, 'Total'));

                $sourceSchedule = data_get($sourceInvoice, 'Schedule', []);
                $targetSchedule = data_get($targetInvoice, 'Schedule', []);

                $sourceLineItems = data_get($sourceInvoice, 'LineItems', []);
                $targetLineItems = data_get($targetInvoice, 'LineItems', []);

                $issues = [];

                if ($sourceContactName !== $targetContactName) {
                    $issues[] = 'Name is not matching';
                }

                if ($sourceTotal !== $targetTotal) {
                    $issues[] = 'Total is not matching';
                }

                $scheduleIssues = $this->compareSchedule($sourceSchedule, $targetSchedule);
                if (!empty($scheduleIssues)) {
                    $issues[] = 'Schedule: ' . implode(', ', $scheduleIssues);
                }

                $lineItemIssues = $this->compareLineItemAccountCodes($sourceLineItems, $targetLineItems);
                if (!empty($lineItemIssues)) {
                    $issues[] = 'LineItems: ' . implode(', ', $lineItemIssues);
                }

                $this->line('Source Contact ID: ' . $sourceContactId);
                $this->line('Target Contact ID: ' . $targetContactId);
                $this->line('Source Contact Name: ' . $sourceContactName);
                $this->line('Target Contact Name: ' . $targetContactName);
                $this->line('Source Total: ' . $this->stringifyValue($sourceTotal));
                $this->line('Target Total: ' . $this->stringifyValue($targetTotal));
                $this->line('Source Schedule: ' . $this->formatSchedule($sourceSchedule));
                $this->line('Target Schedule: ' . $this->formatSchedule($targetSchedule));
                $this->line('Source Account Codes: ' . $this->formatAccountCodes($sourceLineItems));
                $this->line('Target Account Codes: ' . $this->formatAccountCodes($targetLineItems));

                $result = empty($issues) ? 'OK' : 'FLAGGED';
                $message = empty($issues) ? null : implode('; ', $issues);

                if ($result === 'OK') {
                    $this->info('Result: OK');
                    $okCount++;
                } else {
                    $this->warn('Result: ' . $message);
                    $flaggedCount++;
                }

                $completed = $this->storeCheckRecord([
                    'mapping_id' => $mappingId,
                    'source_id' => $sourceRepeatingInvoiceId,
                    'target_id' => $targetRepeatingInvoiceId,
                    'result' => $result,
                    'message' => $message,
                    'source_contact_id' => $sourceContactId !== '' ? $sourceContactId : null,
                    'target_contact_id' => $targetContactId !== '' ? $targetContactId : null,
                    'source_contact_name' => $sourceContactName !== '' ? $sourceContactName : null,
                    'target_contact_name' => $targetContactName !== '' ? $targetContactName : null,
                    'source_total' => $sourceTotal,
                    'target_total' => $targetTotal,
                    'source_schedule' => $this->scheduleSnapshot($sourceSchedule),
                    'target_schedule' => $this->scheduleSnapshot($targetSchedule),
                    'source_account_codes' => $this->extractAccountCodes($sourceLineItems),
                    'target_account_codes' => $this->extractAccountCodes($targetLineItems),
                    'checked_at' => now(),
                ]);

                if ($completed) {
                    $completedThisRun++;
                }
            } catch (Throwable $e) {
                $message = $e->getMessage();

                $this->error('Result: ERROR - ' . $message);

                $completed = $this->storeCheckRecord([
                    'mapping_id' => $mappingId,
                    'source_id' => $sourceRepeatingInvoiceId,
                    'target_id' => $targetRepeatingInvoiceId,
                    'result' => 'ERROR',
                    'message' => $message,
                    'source_contact_id' => null,
                    'target_contact_id' => null,
                    'source_contact_name' => null,
                    'target_contact_name' => null,
                    'source_total' => null,
                    'target_total' => null,
                    'source_schedule' => null,
                    'target_schedule' => null,
                    'source_account_codes' => null,
                    'target_account_codes' => null,
                    'checked_at' => null,
                ]);

                if ($completed) {
                    $completedThisRun++;
                }

                $errorCount++;
            }

            $processedCount++;
            $rowsSincePause++;

            $this->line(str_repeat('-', 80));

            $this->handlePauseIfNeeded(
                $rowsSincePause,
                $batchRows,
                $pauseSeconds,
                $processedCount,
                $totalRowsThisRun,
                $completedBeforeStart + $completedThisRun,
                $totalMappings,
                $abortedByUser
            );

            if ($abortedByUser) {
                break;
            }
        }

        $this->newLine();

        if ($abortedByUser) {
            $this->warn('Check aborted by user.');
        } else {
            $this->info('Check complete.');
        }

        $finalCompleted = $this->countCompletedChecks($checkTableName);

        $this->line('Summary');
        $this->line('-------');
        $this->line('Total mappings: ' . $totalMappings);
        $this->line('Rows processed this run: ' . $processedCount);
        $this->line('OK this run: ' . $okCount);
        $this->line('FLAGGED this run: ' . $flaggedCount);
        $this->line('ERROR this run: ' . $errorCount);
        $this->line('Overall progress: ' . $finalCompleted . ' / ' . $totalMappings);

        return self::SUCCESS;
    }

    protected function runDryRun(
        array $row,
        string $sourceAccessToken,
        string $sourceTenantId,
        string $targetAccessToken,
        string $targetTenantId
    ): int {
        $mappingId = (int) ($row['mapping_id'] ?? 0);
        $sourceRepeatingInvoiceId = trim((string) ($row['source_id'] ?? ''));
        $targetRepeatingInvoiceId = trim((string) ($row['target_id'] ?? ''));

        $this->info('Running dry run with one unchecked mapping row...');
        $this->line('Mapping ID: ' . $mappingId);
        $this->line('Source ID: ' . $sourceRepeatingInvoiceId);
        $this->line('Target ID: ' . $targetRepeatingInvoiceId);
        $this->newLine();

        if ($mappingId <= 0 || $sourceRepeatingInvoiceId === '' || $targetRepeatingInvoiceId === '') {
            $this->error('Dry run failed: mapping_id, source_id, or target_id is empty.');
            return self::FAILURE;
        }

        try {
            $sourceInvoice = $this->fetchRepeatingInvoice(
                $sourceAccessToken,
                $sourceTenantId,
                $sourceRepeatingInvoiceId,
                'SOURCE'
            );

            $targetInvoice = $this->fetchRepeatingInvoice(
                $targetAccessToken,
                $targetTenantId,
                $targetRepeatingInvoiceId,
                'TARGET'
            );

            if (!$sourceInvoice) {
                $this->error('Dry run failed: source repeating invoice not found.');
                return self::FAILURE;
            }

            if (!$targetInvoice) {
                $this->error('Dry run failed: target repeating invoice not found.');
                return self::FAILURE;
            }

            $this->info('Dry run successful.');
            $this->line('SOURCE Contact: ' . trim((string) data_get($sourceInvoice, 'Contact.Name', '')));
            $this->line('TARGET Contact: ' . trim((string) data_get($targetInvoice, 'Contact.Name', '')));

            return self::SUCCESS;
        } catch (Throwable $e) {
            $this->error('Dry run failed: ' . $e->getMessage());
            return self::FAILURE;
        }
    }

    protected function assertTables(string $tableName, string $checkTableName): void
    {
        if ($tableName === '') {
            throw new RuntimeException('Mapping table name cannot be empty.');
        }

        if ($checkTableName === '') {
            throw new RuntimeException('Check table name cannot be empty.');
        }

        if (!DB::getSchemaBuilder()->hasTable($tableName)) {
            throw new RuntimeException('Mapping table not found: ' . $tableName);
        }

        if (!DB::getSchemaBuilder()->hasTable($checkTableName)) {
            throw new RuntimeException('Check table not found: ' . $checkTableName);
        }

        foreach (['id', 'source_id', 'target_id'] as $column) {
            if (!DB::getSchemaBuilder()->hasColumn($tableName, $column)) {
                throw new RuntimeException("Mapping table [{$tableName}] must contain {$column}.");
            }
        }

        foreach (['mapping_id', 'checked_at'] as $column) {
            if (!DB::getSchemaBuilder()->hasColumn($checkTableName, $column)) {
                throw new RuntimeException("Check table [{$checkTableName}] must contain {$column}.");
            }
        }
    }

    protected function countTotalMappings(string $tableName): int
    {
        return (int) DB::table($tableName)
            ->whereNotNull('source_id')
            ->whereNotNull('target_id')
            ->count();
    }

    protected function countCompletedChecks(string $checkTableName): int
    {
        return (int) DB::table($checkTableName)
            ->whereNotNull('checked_at')
            ->count();
    }

    protected function loadUncheckedRowsFromMappingTable(string $tableName, string $checkTableName): array
    {
        $records = DB::table($tableName . ' as m')
            ->leftJoin($checkTableName . ' as c', 'c.mapping_id', '=', 'm.id')
            ->whereNotNull('m.source_id')
            ->whereNotNull('m.target_id')
            ->where(function ($query) {
                $query->whereNull('c.mapping_id')
                    ->orWhereNull('c.checked_at');
            })
            ->orderBy('m.id')
            ->select([
                'm.id as mapping_id',
                'm.source_id',
                'm.target_id',
            ])
            ->get();

        $rows = [];

        foreach ($records as $index => $record) {
            $rows[] = [
                '_original_line' => $index + 1,
                'mapping_id' => (int) ($record->mapping_id ?? 0),
                'source_id' => trim((string) ($record->source_id ?? '')),
                'target_id' => trim((string) ($record->target_id ?? '')),
            ];
        }

        return $rows;
    }

    protected function storeCheckRecord(array $attributes): bool
    {
        if (empty($attributes['mapping_id'])) {
            return false;
        }

        $record = XeroRepeatingInvoiceCheck::updateOrCreate(
            ['mapping_id' => $attributes['mapping_id']],
            $attributes
        );

        return $record->checked_at !== null;
    }

    protected function handlePauseIfNeeded(
        int &$rowsSincePause,
        int $batchRows,
        int $pauseSeconds,
        int $processedCount,
        int $totalRowsThisRun,
        int $overallCompleted,
        int $totalMappings,
        bool &$abortedByUser
    ): void {
        if ($rowsSincePause < $batchRows) {
            return;
        }

        if ($processedCount >= $totalRowsThisRun) {
            return;
        }

        $this->newLine();
        $this->warn("Processed {$rowsSincePause} rows in this batch.");
        $this->line("Run progress: {$processedCount} / {$totalRowsThisRun}");
        $this->line("Overall progress: {$overallCompleted} / {$totalMappings}");

        $abort = $this->confirm('Do you want to abort?', false);

        if ($abort) {
            $abortedByUser = true;
            return;
        }

        $this->line("Pausing {$pauseSeconds} seconds...");
        sleep($pauseSeconds);

        $rowsSincePause = 0;
    }

    protected function fetchRepeatingInvoice(
        string $accessToken,
        string $tenantId,
        string $repeatingInvoiceId,
        string $label
    ): ?array {
        $url = 'https://api.xero.com/api.xro/2.0/RepeatingInvoices/' . $repeatingInvoiceId;
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

                if (!isset($data['RepeatingInvoices']) || !is_array($data['RepeatingInvoices']) || empty($data['RepeatingInvoices'])) {
                    return null;
                }

                return $data['RepeatingInvoices'][0];
            }

            if ($response->status() === 429) {
                $retryAfter = (int) ($response->header('Retry-After') ?? 5);
                $retryAfter = $retryAfter > 0 ? $retryAfter : 5;

                $this->warn("Rate limited while fetching {$label} repeating invoice {$repeatingInvoiceId}. Waiting {$retryAfter} seconds before retry {$attempt}/{$maxAttempts}...");
                sleep($retryAfter);
                $attempt++;
                continue;
            }

            throw new RuntimeException(
                'Xero API request failed for ' . $label . ' repeating invoice [' . $repeatingInvoiceId . '] with status '
                . $response->status() . ': ' . $response->body()
            );
        }

        throw new RuntimeException(
            'Rate limited while fetching ' . $label . ' repeating invoice [' . $repeatingInvoiceId . '] after retries.'
        );
    }

    protected function compareSchedule($sourceSchedule, $targetSchedule): array
    {
        $fields = [
            'Period' => 'Period',
            'Unit' => 'Unit',
            'DueDate' => 'DueDate',
            'DueDateType' => 'DueDateType',
            'NextScheduledDate' => 'NextScheduledDate',
            'NextScheduledDateString' => 'NextScheduledDateString',
        ];

        $issues = [];

        foreach ($fields as $key => $label) {
            $sourceValue = data_get($sourceSchedule, $key);
            $targetValue = data_get($targetSchedule, $key);

            if ($sourceValue !== $targetValue) {
                $issues[] = $label;
            }
        }

        return $issues;
    }

    protected function compareLineItemAccountCodes($sourceLineItems, $targetLineItems): array
    {
        $issues = [];

        $sourceCount = is_array($sourceLineItems) ? count($sourceLineItems) : 0;
        $targetCount = is_array($targetLineItems) ? count($targetLineItems) : 0;

        if ($sourceCount !== $targetCount) {
            $issues[] = 'Line item count is not matching';
        }

        $maxCount = max($sourceCount, $targetCount);

        for ($i = 0; $i < $maxCount; $i++) {
            $sourceAccountCode = trim((string) data_get($sourceLineItems, $i . '.AccountCode', ''));
            $targetAccountCode = trim((string) data_get($targetLineItems, $i . '.AccountCode', ''));

            if ($sourceAccountCode !== $targetAccountCode) {
                $issues[] = 'AccountCode mismatch at line ' . ($i + 1);
            }
        }

        return $issues;
    }

    protected function formatSchedule($schedule): string
    {
        return implode(', ', [
            'Period=' . $this->stringifyValue(data_get($schedule, 'Period')),
            'Unit=' . $this->stringifyValue(data_get($schedule, 'Unit')),
            'DueDate=' . $this->stringifyValue(data_get($schedule, 'DueDate')),
            'DueDateType=' . $this->stringifyValue(data_get($schedule, 'DueDateType')),
            'StartDate=' . $this->stringifyValue(data_get($schedule, 'StartDate')),
            'NextScheduledDate=' . $this->stringifyValue(data_get($schedule, 'NextScheduledDate')),
            'NextScheduledDateString=' . $this->stringifyValue(data_get($schedule, 'NextScheduledDateString')),
        ]);
    }

    protected function formatAccountCodes($lineItems): string
    {
        if (!is_array($lineItems) || empty($lineItems)) {
            return '(none)';
        }

        $parts = [];

        foreach (array_values($lineItems) as $index => $lineItem) {
            $parts[] = '[' . ($index + 1) . '] ' . trim((string) data_get($lineItem, 'AccountCode', ''));
        }

        return implode(', ', $parts);
    }

    protected function scheduleSnapshot($schedule): ?array
    {
        if (!is_array($schedule) || empty($schedule)) {
            return null;
        }

        return [
            'Period' => data_get($schedule, 'Period'),
            'Unit' => data_get($schedule, 'Unit'),
            'DueDate' => data_get($schedule, 'DueDate'),
            'DueDateType' => data_get($schedule, 'DueDateType'),
            'StartDate' => data_get($schedule, 'StartDate'),
            'NextScheduledDate' => data_get($schedule, 'NextScheduledDate'),
            'NextScheduledDateString' => data_get($schedule, 'NextScheduledDateString'),
        ];
    }

    protected function extractAccountCodes($lineItems): ?array
    {
        if (!is_array($lineItems) || empty($lineItems)) {
            return null;
        }

        $codes = [];

        foreach (array_values($lineItems) as $index => $lineItem) {
            $codes[] = [
                'line' => $index + 1,
                'account_code' => trim((string) data_get($lineItem, 'AccountCode', '')),
            ];
        }

        return $codes;
    }

    protected function normalizeDecimal($value): ?string
    {
        if ($value === null || $value === '') {
            return null;
        }

        return number_format((float) $value, 4, '.', '');
    }

    protected function stringifyValue($value): string
    {
        if ($value === null) {
            return '';
        }

        if (is_scalar($value)) {
            return (string) $value;
        }

        return json_encode($value, JSON_UNESCAPED_UNICODE | JSON_UNESCAPED_SLASHES) ?: '';
    }

    private function getTenantId(XeroOrganisation $organisation): ?string
    {
        return $organisation->xero_tenant_id
            ?? $organisation->tenant_id
            ?? null;
    }
}