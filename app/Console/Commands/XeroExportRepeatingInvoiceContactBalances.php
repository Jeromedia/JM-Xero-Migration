<?php

namespace App\Console\Commands;

use App\Models\XeroOrganisation;
use App\Models\XeroToken;
use Carbon\Carbon;
use Illuminate\Console\Command;
use Illuminate\Http\Client\Response;
use Illuminate\Support\Facades\Http;
use Illuminate\Support\Facades\Storage;
use RuntimeException;
use Throwable;

class XeroExportRepeatingInvoiceContactBalances extends Command
{
    protected $signature = 'xero:repeating-invoices:export-contact-balances
                            {--source=source : XeroOrganisation role to use as source}
                            {--path= : Optional custom output path/filename}
                            {--include-deleted : Include deleted repeating invoice templates if returned}
                            {--batch=50 : Number of ContactIDs per filtered contacts request}
                            {--sleep-ms=200 : Delay in milliseconds between contact batch requests}
                            {--dry-run : Do everything except writing the CSV file}';

    protected $description = 'Export one CSV row per contact for customers that have repeating invoices, including balances and repeating invoice counts';

    public function handle(): int
    {
        $this->info('Starting export of contact balances for customers with repeating invoices...');

        try {
            $sourceRole = (string) $this->option('source');
            $organisation = XeroOrganisation::where('role', $sourceRole)->first();

            if (!$organisation) {
                $this->error("No Xero organisation found with role '{$sourceRole}'.");
                return self::FAILURE;
            }

            $token = XeroToken::orderByDesc('id')->first();

            if (!$token || empty($token->access_token)) {
                $this->error('No valid Xero access token found.');
                return self::FAILURE;
            }

            if ($token->expires_at && Carbon::parse($token->expires_at)->isPast()) {
                $this->error('The Xero access token appears to be expired. Refresh it first, then run this command again.');
                return self::FAILURE;
            }

            $tenantId = $organisation->tenant_id;

            if (!$tenantId) {
                $this->error("The organisation with role '{$sourceRole}' does not have a tenant_id.");
                return self::FAILURE;
            }

            $batchSize = max(1, (int) $this->option('batch'));
            $sleepMs = max(0, (int) $this->option('sleep-ms'));
            $includeDeleted = (bool) $this->option('include-deleted');

            $this->line("Source role: {$sourceRole}");
            $this->line('Source tenant: ' . ($organisation->name ?: '[no name]') . " ({$tenantId})");
            $this->line("Contact batch size: {$batchSize}");
            $this->line("Sleep between contact batch requests: {$sleepMs} ms");

            $repeatingInvoices = $this->fetchRepeatingInvoices($token->access_token, $tenantId);

            $this->info('Total repeating invoice templates fetched: ' . count($repeatingInvoices));

            if (empty($repeatingInvoices)) {
                $this->warn('No repeating invoices found.');
                return self::SUCCESS;
            }

            $contactSummary = $this->buildContactSummaryFromRepeatingInvoices($repeatingInvoices, $includeDeleted);

            $uniqueContactIds = array_keys($contactSummary);

            $this->info('Unique contacts with repeating invoices: ' . count($uniqueContactIds));

            if (empty($uniqueContactIds)) {
                $this->warn('No contacts found on repeating invoices after filtering.');
                return self::SUCCESS;
            }

            $contactsById = $this->fetchContactsByIds(
                $token->access_token,
                $tenantId,
                $uniqueContactIds,
                $batchSize,
                $sleepMs
            );

            $this->info('Matched contacts fetched from Contacts endpoint: ' . count($contactsById));

            $csvRows = [];

            foreach ($contactSummary as $contactId => $summary) {
                $contact = $contactsById[$contactId] ?? null;
                $balances = data_get($contact, 'Balances', []);

                $accountsReceivable = data_get($balances, 'AccountsReceivable', []);
                $accountsPayable = data_get($balances, 'AccountsPayable', []);

                $csvRows[] = [
                    'contact_name' => $summary['contact_name'],
                    'contact_id' => $contactId,
                    'repeating_invoice_count' => $summary['repeating_invoice_count'],
                    'active_template_count' => $summary['active_template_count'],
                    'draft_template_count' => $summary['draft_template_count'],
                    'authorised_template_count' => $summary['authorised_template_count'],
                    'deleted_template_count' => $summary['deleted_template_count'],
                    'first_next_scheduled_date' => $summary['first_next_scheduled_date'],
                    'last_next_scheduled_date' => $summary['last_next_scheduled_date'],
                    'template_currency_codes' => implode(', ', $summary['currency_codes']),
                    'template_references' => implode(' | ', $summary['references']),

                    'ar_outstanding' => data_get($accountsReceivable, 'Outstanding', ''),
                    'ar_overdue' => data_get($accountsReceivable, 'Overdue', ''),
                    'ap_outstanding' => data_get($accountsPayable, 'Outstanding', ''),
                    'ap_overdue' => data_get($accountsPayable, 'Overdue', ''),

                    'contact_status' => data_get($contact, 'ContactStatus', ''),
                    'email_address' => data_get($contact, 'EmailAddress', ''),
                    'first_name' => data_get($contact, 'FirstName', ''),
                    'last_name' => data_get($contact, 'LastName', ''),
                    'updated_date_utc' => $this->normaliseXeroDate(data_get($contact, 'UpdatedDateUTC', '')),
                ];
            }

            usort($csvRows, function (array $a, array $b) {
                return strcasecmp((string) $a['contact_name'], (string) $b['contact_name']);
            });

            $this->info('CSV rows prepared: ' . count($csvRows));

            if ((bool) $this->option('dry-run')) {
                $this->warn('Dry run enabled. CSV file was not written.');
                return self::SUCCESS;
            }

            $path = $this->option('path');
            if (!$path) {
                $timestamp = now()->format('Ymd_His');
                $path = "xero_exports/repeating_invoice_contact_balances_{$timestamp}.csv";
            }

            $this->writeCsv($path, $csvRows);

            $fullPath = storage_path('app/' . $path);

            $this->info('CSV export completed successfully.');
            $this->line("File written to: {$fullPath}");

            return self::SUCCESS;
        } catch (Throwable $e) {
            $this->error('Export failed: ' . $e->getMessage());
            $this->newLine();
            $this->line($e->getTraceAsString());

            return self::FAILURE;
        }
    }

    protected function fetchRepeatingInvoices(string $accessToken, string $tenantId): array
    {
        $this->line('Fetching repeating invoices...');

        $response = $this->xeroGet(
            'https://api.xero.com/api.xro/2.0/RepeatingInvoices',
            $accessToken,
            $tenantId
        );

        return $response->json('RepeatingInvoices', []);
    }

    protected function buildContactSummaryFromRepeatingInvoices(array $repeatingInvoices, bool $includeDeleted): array
    {
        $summary = [];

        foreach ($repeatingInvoices as $template) {
            $status = strtoupper((string) data_get($template, 'Status', ''));
            $contactId = data_get($template, 'Contact.ContactID');
            $contactName = (string) data_get($template, 'Contact.Name', '');

            if (!$contactId) {
                continue;
            }

            if (!$includeDeleted && $status === 'DELETED') {
                continue;
            }

            if (!isset($summary[$contactId])) {
                $summary[$contactId] = [
                    'contact_name' => $contactName,
                    'repeating_invoice_count' => 0,
                    'active_template_count' => 0,
                    'draft_template_count' => 0,
                    'authorised_template_count' => 0,
                    'deleted_template_count' => 0,
                    'next_dates_raw' => [],
                    'currency_codes' => [],
                    'references' => [],
                ];
            }

            $summary[$contactId]['repeating_invoice_count']++;

            if ($status === 'ACTIVE') {
                $summary[$contactId]['active_template_count']++;
            }

            if ($status === 'DRAFT') {
                $summary[$contactId]['draft_template_count']++;
            }

            if ($status === 'AUTHORISED') {
                $summary[$contactId]['authorised_template_count']++;
            }

            if ($status === 'DELETED') {
                $summary[$contactId]['deleted_template_count']++;
            }

            $nextDate = $this->normaliseXeroDate(data_get($template, 'Schedule.NextScheduledDate', ''));
            if ($nextDate !== '') {
                $summary[$contactId]['next_dates_raw'][] = $nextDate;
            }

            $currencyCode = trim((string) data_get($template, 'CurrencyCode', ''));
            if ($currencyCode !== '') {
                $summary[$contactId]['currency_codes'][$currencyCode] = $currencyCode;
            }

            $reference = trim((string) data_get($template, 'Reference', ''));
            if ($reference !== '') {
                $summary[$contactId]['references'][$reference] = $reference;
            }

            if ($summary[$contactId]['contact_name'] === '' && $contactName !== '') {
                $summary[$contactId]['contact_name'] = $contactName;
            }
        }

        foreach ($summary as $contactId => $row) {
            $dates = $row['next_dates_raw'];
            sort($dates);

            $summary[$contactId]['first_next_scheduled_date'] = $dates[0] ?? '';
            $summary[$contactId]['last_next_scheduled_date'] = !empty($dates) ? $dates[count($dates) - 1] : '';
            $summary[$contactId]['currency_codes'] = array_values($row['currency_codes']);
            $summary[$contactId]['references'] = array_values($row['references']);

            unset($summary[$contactId]['next_dates_raw']);
        }

        return $summary;
    }

    protected function fetchContactsByIds(
        string $accessToken,
        string $tenantId,
        array $contactIds,
        int $batchSize,
        int $sleepMs
    ): array {
        $this->line('Fetching contacts in filtered batches...');

        $chunks = array_chunk($contactIds, $batchSize);
        $found = [];
        $totalChunks = count($chunks);

        foreach ($chunks as $index => $chunk) {
            $chunkNumber = $index + 1;
            $this->line("Fetching contact batch {$chunkNumber} of {$totalChunks} (" . count($chunk) . ' ContactIDs)...');

            $response = $this->xeroGet(
                'https://api.xero.com/api.xro/2.0/Contacts',
                $accessToken,
                $tenantId,
                [
                    'summaryOnly' => 'false',
                    'IDs' => $this->buildContactsIdsParam($chunk),
                ]
            );

            $contacts = $response->json('Contacts', []);

            foreach ($contacts as $contact) {
                $contactId = data_get($contact, 'ContactID');
                if ($contactId) {
                    $found[$contactId] = $contact;
                }
            }

            $this->line('Matched so far: ' . count($found) . ' / ' . count($contactIds));

            if ($sleepMs > 0 && $chunkNumber < $totalChunks) {
                usleep($sleepMs * 1000);
            }
        }

        return $found;
    }

    protected function buildContactsIdsParam(array $contactIds): string
    {
        return implode(',', array_map(
            static fn($id) => trim((string) $id),
            $contactIds
        ));
    }


    protected function xeroGet(
        string $url,
        string $accessToken,
        string $tenantId,
        array $query = [],
        int $maxAttempts = 4
    ): Response {
        $attempt = 0;

        do {
            $attempt++;

            $response = Http::withToken($accessToken)
                ->withHeaders([
                    'Xero-tenant-id' => $tenantId,
                    'Accept' => 'application/json',
                ])
                ->timeout(120)
                ->get($url, $query);

            $this->logRateLimitHeaders($response);

            if ($response->successful()) {
                return $response;
            }

            if ($response->status() === 429 && $attempt < $maxAttempts) {
                $retryAfter = (int) ($response->header('Retry-After') ?? 5);
                $retryAfter = max(1, $retryAfter);

                $this->warn("Xero rate limit hit (429). Waiting {$retryAfter} second(s) before retry {$attempt}/" . ($maxAttempts - 1) . '...');
                sleep($retryAfter);
                continue;
            }

            $this->error('Xero request failed.');
            $this->line('URL: ' . $url);

            if (!empty($query)) {
                $this->line('Query: ' . json_encode($query, JSON_UNESCAPED_SLASHES));
            }

            $this->line('HTTP Status: ' . $response->status());
            $this->line($response->body());

            throw new RuntimeException('Xero API request failed with status ' . $response->status() . '.');
        } while ($attempt < $maxAttempts);

        throw new RuntimeException('Xero API request failed after retries.');
    }

    protected function logRateLimitHeaders(Response $response): void
    {
        $minuteRemaining = $response->header('X-MinLimit-Remaining');
        $dayRemaining = $response->header('X-DayLimit-Remaining');

        if ($minuteRemaining !== null || $dayRemaining !== null) {
            $this->line(
                'Rate limits remaining -> minute: ' . ($minuteRemaining ?? 'n/a')
                    . ' | day: ' . ($dayRemaining ?? 'n/a')
            );
        }
    }

    protected function writeCsv(string $path, array $rows): void
    {
        if (empty($rows)) {
            throw new RuntimeException('No rows available to write to CSV.');
        }

        $directory = dirname($path);

        if ($directory && $directory !== '.') {
            Storage::disk('local')->makeDirectory($directory);
        }

        $stream = fopen('php://temp', 'w+');

        if ($stream === false) {
            throw new RuntimeException('Unable to open temporary stream for CSV generation.');
        }

        fputcsv($stream, array_keys($rows[0]));

        foreach ($rows as $row) {
            fputcsv($stream, $row);
        }

        rewind($stream);
        $csvContent = stream_get_contents($stream);
        fclose($stream);

        Storage::disk('local')->put($path, $csvContent);
    }

    protected function normaliseXeroDate(?string $value): string
    {
        if (!$value) {
            return '';
        }

        if (preg_match('/\/Date\((\d+)(?:[+-]\d+)?\)\//', $value, $matches)) {
            $timestampMs = (int) $matches[1];
            return Carbon::createFromTimestampMs($timestampMs)->format('Y-m-d H:i:s');
        }

        try {
            return Carbon::parse($value)->format('Y-m-d H:i:s');
        } catch (Throwable $e) {
            return (string) $value;
        }
    }
}
