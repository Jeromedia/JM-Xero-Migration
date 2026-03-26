<?php

namespace App\Console\Commands\RepeatingInvoices;

use App\Models\XeroOrganisation;
use Illuminate\Console\Command;
use Illuminate\Support\Facades\Http;
use Throwable;

class XeroValidateRepeatingInvoicePairs extends Command
{
    protected $signature = 'xero:repeating-invoices:validate-pairs
                            {file=private/xero_imports/repeating_invoice_pairs.csv : Path under storage/app/}
                            {--source=source : Source organisation role}
                            {--target=target : Target organisation role}
                            {--first : Process only the first CSV row}
                            {--random : Process only one random CSV row}';

    protected $description = 'Validate repeating invoice pairs from CSV by comparing Contact Name, Total, Schedule, and Line Item Account Codes between source and target';

    public function handle(): int
    {
        $filePath = $this->argument('file');
        $sourceRole = (string) $this->option('source');
        $targetRole = (string) $this->option('target');
        $useFirst = (bool) $this->option('first');
        $useRandom = (bool) $this->option('random');

        if ($useFirst && $useRandom) {
            $this->error('Use only one of --first or --random, not both.');
            return self::FAILURE;
        }

        $this->info('Starting repeating invoice pair validation...');
        $this->line('Input file: storage/app/' . $filePath);
        $this->line('Source role: ' . $sourceRole);
        $this->line('Target role: ' . $targetRole);

        if ($useFirst) {
            $this->line('Mode: FIRST ROW ONLY');
        } elseif ($useRandom) {
            $this->line('Mode: RANDOM ROW ONLY');
        } else {
            $this->line('Mode: ALL ROWS');
        }

        $this->newLine();

        $fullPath = storage_path('app/' . $filePath);

        if (!file_exists($fullPath)) {
            $this->error('Input file not found: ' . $fullPath);
            return self::FAILURE;
        }

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

        $rows = $this->parseInputFile((string) file_get_contents($fullPath));

        if (empty($rows)) {
            $this->error('No valid rows found in input file.');
            return self::FAILURE;
        }

        if ($useFirst) {
            $rows = [array_values($rows)[0]];
        } elseif ($useRandom) {
            $randomKey = array_rand($rows);
            $rows = [$rows[$randomKey]];
        }

        $okCount = 0;
        $flaggedCount = 0;
        $errorCount = 0;

        foreach (array_values($rows) as $index => $row) {
            $lineNumber = (int) ($row['_original_line'] ?? ($index + 1));

            $sourceRepeatingInvoiceId = trim((string) ($row['source_id'] ?? ''));
            $targetRepeatingInvoiceId = trim((string) ($row['target_id'] ?? ''));

            $this->newLine();
            $this->info('Line ' . $lineNumber);

            if ($sourceRepeatingInvoiceId === '' || $targetRepeatingInvoiceId === '') {
                $this->error('Result: Missing Source ID or Target ID in CSV');
                $errorCount++;
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
                    $this->error('Result: Source repeating invoice not found');
                    $errorCount++;
                    continue;
                }

                if (!$targetInvoice) {
                    $this->error('Result: Target repeating invoice not found');
                    $errorCount++;
                    continue;
                }

                $sourceContactId = trim((string) data_get($sourceInvoice, 'Contact.ContactID', ''));
                $targetContactId = trim((string) data_get($targetInvoice, 'Contact.ContactID', ''));
                $sourceContactName = trim((string) data_get($sourceInvoice, 'Contact.Name', ''));
                $targetContactName = trim((string) data_get($targetInvoice, 'Contact.Name', ''));
                $sourceTotal = data_get($sourceInvoice, 'Total');
                $targetTotal = data_get($targetInvoice, 'Total');

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

                if (empty($issues)) {
                    $this->info('Result: OK');
                    $okCount++;
                } else {
                    $this->warn('Result: ' . implode('; ', $issues));
                    $flaggedCount++;
                }
            } catch (Throwable $e) {
                $this->error('Result: ERROR - ' . $e->getMessage());
                $errorCount++;
            }

            $this->line(str_repeat('-', 80));
        }

        $this->newLine();
        $this->info('Validation complete.');
        $this->line('Summary');
        $this->line('-------');
        $this->line('Rows checked: ' . count($rows));
        $this->line('OK: ' . $okCount);
        $this->line('FLAGGED: ' . $flaggedCount);
        $this->line('ERROR: ' . $errorCount);

        return self::SUCCESS;
    }

    protected function parseInputFile(string $content): array
    {
        $content = trim($content);

        if ($content === '') {
            return [];
        }

        $lines = preg_split('/\r\n|\r|\n/', $content);
        $lines = array_values(array_filter(array_map('trim', $lines), fn ($line) => $line !== ''));

        if (empty($lines)) {
            return [];
        }

        $delimiter = $this->detectDelimiter($lines[0]);
        $header = str_getcsv(array_shift($lines), $delimiter);
        $header = array_map(fn ($value) => $this->normalizeHeader($value), $header);

        $rows = [];

        foreach ($lines as $index => $line) {
            $values = str_getcsv($line, $delimiter);

            if (count($values) === 1 && str_contains($line, "\t")) {
                $values = str_getcsv($line, "\t");
            }

            $row = [];
            foreach ($header as $i => $column) {
                $row[$column] = isset($values[$i]) ? trim((string) $values[$i]) : '';
            }

            $rows[] = [
                '_original_line' => $index + 2,
                'source_id' => trim((string) ($row['source_id'] ?? '')),
                'target_id' => trim((string) ($row['target_id'] ?? '')),
            ];
        }

        return $rows;
    }

    protected function detectDelimiter(string $line): string
    {
        if (str_contains($line, "\t")) {
            return "\t";
        }

        if (substr_count($line, ';') > substr_count($line, ',')) {
            return ';';
        }

        return ',';
    }

    protected function normalizeHeader(string $value): string
    {
        $value = trim($value);
        $value = strtolower($value);
        $value = preg_replace('/[^a-z0-9]+/', '_', $value);

        return trim((string) $value, '_');
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

            throw new \RuntimeException(
                'Xero API request failed for ' . $label . ' repeating invoice [' . $repeatingInvoiceId . '] with status '
                . $response->status() . ': ' . $response->body()
            );
        }

        throw new \RuntimeException(
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
            // 'StartDate' => 'StartDate',
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