<?php

namespace App\Console\Commands;

use App\Models\XeroOrganisation;
use App\Models\XeroToken;
use Carbon\Carbon;
use Illuminate\Console\Command;
use Illuminate\Support\Facades\Http;
use Illuminate\Support\Facades\Storage;
use Throwable;

class XeroListDirectDebitRepeating extends Command
{
    protected $signature = 'xero:list-dd {role : source|target}';
    protected $description = 'READ-ONLY: Export repeating invoices for Direct Debit groups (one group at a time) to CSV';

    // Hard-coded group names (from client screenshot)
    private const GROUP_NAMES = [
        'Bank Direct Debit - All Invoices',
        'Bank Direct Debit - Mon Inv Only',
        'CC Direct Debit - All Invoices',
        'CC Direct Debit - Mon Inv Only',
    ];

    // TEST RANGE (hard-coded): 2026-03-20 -> 2026-03-22
    private const TEST_YEAR = 2026;
    private const FROM_MONTH = 3;
    private const FROM_DAY   = 20;
    private const TO_MONTH   = 3;
    private const TO_DAY     = 27;

    public function handle(): int
    {
        try {
            return $this->runReport();
        } catch (Throwable $e) {
            $this->error('Fatal error: ' . $e->getMessage());
            $this->line('File: ' . $e->getFile() . ':' . $e->getLine());
            return self::FAILURE;
        }
    }

    private function runReport(): int
    {
        $role = (string) $this->argument('role');
        if (!in_array($role, ['source', 'target'], true)) {
            $this->error('Role must be source or target.');
            return self::FAILURE;
        }

        $from = Carbon::create(self::TEST_YEAR, self::FROM_MONTH, self::FROM_DAY)->startOfDay();
        $to   = Carbon::create(self::TEST_YEAR, self::TO_MONTH, self::TO_DAY)->endOfDay();

        $org = XeroOrganisation::where('role', $role)->first();
        if (!$org) {
            $this->error("No organisation set for role={$role}. Run: php artisan xero:connections");
            return self::FAILURE;
        }

        $token = XeroToken::find($org->xero_token_id);
        if (!$token) {
            $this->error("Token not found. Reconnect to Xero.");
            return self::FAILURE;
        }

        // READ-ONLY command: no refresh here
        if (!$token->expires_at || $token->expires_at->isPast()) {
            $this->error('Access token expired.');
            $this->line('Run: php artisan xero:token-refresh');
            return self::FAILURE;
        }

        $tenantId = $org->tenant_id;

        $this->info('=== Xero Direct Debit Repeating Invoices (CSV export) ===');
        $this->line("Role: {$role}");
        $this->line("Org: " . ($org->tenant_name ?? '(unknown)'));
        $this->line("TenantId: {$tenantId}");
        $this->line("Date range: {$from->toDateString()} -> {$to->toDateString()}");

        /*
        |--------------------------------------------------------------------------
        | Step A: Fetch Contact Groups (once)
        |--------------------------------------------------------------------------
        */
        $this->line('');
        $this->line('Step A) Fetching Contact Groups...');

        $resp = $this->client($token->access_token, $tenantId)
            ->get('https://api.xero.com/api.xro/2.0/ContactGroups');

        if (!$resp->successful()) {
            $this->error('Failed fetching ContactGroups. HTTP ' . $resp->status());
            $this->line($resp->body());
            return self::FAILURE;
        }

        $allGroups = $resp->json()['ContactGroups'] ?? [];

        // Map group name => group id (only for our 4 hard-coded groups)
        $groupMap = [];
        foreach ($allGroups as $g) {
            $name = $g['Name'] ?? null;
            $id   = $g['ContactGroupID'] ?? null;
            if ($name && $id && in_array($name, self::GROUP_NAMES, true)) {
                $groupMap[$name] = $id;
            }
        }

        $this->info('✓ Matched groups: ' . count($groupMap) . ' / ' . count(self::GROUP_NAMES));

        if (count($groupMap) === 0) {
            $this->warn('No matching Direct Debit groups found. Check the hard-coded names.');
            return self::SUCCESS;
        }

        /*
        |--------------------------------------------------------------------------
        | Step B: Fetch RepeatingInvoices ONCE (no paging)
        |--------------------------------------------------------------------------
        | We do this once because your Xero tenant is returning the same count for
        | every "page" call, meaning paging is ignored / returns the same dataset.
        */
        $this->line('');
        $this->line('Step B) Fetching repeating invoices (single request, no paging)...');

        $resp = $this->client($token->access_token, $tenantId)
            ->get('https://api.xero.com/api.xro/2.0/RepeatingInvoices');

        if (!$resp->successful()) {
            $this->error('Failed fetching RepeatingInvoices. HTTP ' . $resp->status());
            $this->line($resp->body());
            return self::FAILURE;
        }

        $allRepeating = $resp->json()['RepeatingInvoices'] ?? [];
        $this->info('✓ Repeating invoices fetched: ' . count($allRepeating));

        /*
        |--------------------------------------------------------------------------
        | Process groups one by one
        |--------------------------------------------------------------------------
        */
        $groupNamesInOrder = array_values(array_intersect(self::GROUP_NAMES, array_keys($groupMap)));
        $totalGroups = count($groupNamesInOrder);

        foreach ($groupNamesInOrder as $index => $groupName) {
            $gid = $groupMap[$groupName];
            $groupNo = $index + 1;

            $this->line('');
            $this->info('==============================================');
            $this->info("Group {$groupNo}/{$totalGroups}: {$groupName}");
            $this->info('==============================================');

            if (!$this->confirm("Start Group {$groupNo}/{$totalGroups} now?", true)) {
                $this->warn('Skipped: ' . $groupName);
                continue;
            }

            /*
            |--------------------------------------------------------------------------
            | Step C: Fetch contacts in THIS group
            |--------------------------------------------------------------------------
            */
            $this->line('Step C) Fetching contacts in this group...');

            $groupResp = $this->client($token->access_token, $tenantId)
                ->get("https://api.xero.com/api.xro/2.0/ContactGroups/{$gid}");

            if (!$groupResp->successful()) {
                $this->error("Failed fetching ContactGroup {$gid}. HTTP " . $groupResp->status());
                $this->line($groupResp->body());
                return self::FAILURE;
            }

            $contacts = $groupResp->json()['ContactGroups'][0]['Contacts'] ?? [];
            $contactIds = [];
            foreach ($contacts as $c) {
                if (!empty($c['ContactID'])) {
                    $contactIds[$c['ContactID']] = true;
                }
            }

            $this->info('✓ Contacts collected: ' . count($contactIds));

            if (count($contactIds) === 0) {
                $this->warn('No contacts in this group.');
                if (!$this->confirm('Continue to next group?', true)) break;
                continue;
            }

            /*
            |--------------------------------------------------------------------------
            | Step D: Filter repeating invoices for this group
            |--------------------------------------------------------------------------
            */
            $this->line('Step D) Filtering repeating invoices for this group...');
            $this->line('  Date filter: ' . $from->toDateString() . ' -> ' . $to->toDateString());

            $results = [];
            $scanned = 0;

            foreach ($allRepeating as $inv) {
                $scanned++;

                $cid = $inv['Contact']['ContactID'] ?? null;
                if (!$cid || !isset($contactIds[$cid])) continue;

                $nextRaw = $inv['Schedule']['NextScheduledDate'] ?? null;
                if (!$nextRaw) continue;

                $next = $this->parseXeroDate($nextRaw);
                if (!$next) continue;

                if ($next->between($from, $to)) {
                    $results[] = [
                        'Group'              => $groupName,
                        'RepeatingInvoiceID' => $inv['RepeatingInvoiceID'] ?? '',
                        'Contact'            => $inv['Contact']['Name'] ?? '',
                        'NextScheduledDate'  => $next->toDateString(),
                        'Reference'          => $inv['Reference'] ?? '',
                        'Status'             => $inv['Status'] ?? '',
                    ];
                }
            }

            $this->info("✓ Repeating invoices scanned: {$scanned}");
            $this->info('✓ Matches found for this group: ' . count($results));

            /*
            |--------------------------------------------------------------------------
            | Step E: Write CSV (one file per group)
            |--------------------------------------------------------------------------
            */
            $path = $this->writeCsv($groupName, $from, $to, $results);
            $this->info('✓ CSV saved: ' . $path);

            // Preview first 20
            $preview = array_slice($results, 0, 20);
            if (!empty($preview)) {
                $this->table(
                    ['RepeatingInvoiceID', 'Contact', 'NextScheduledDate', 'Reference', 'Status'],
                    array_map(fn ($r) => [
                        $r['RepeatingInvoiceID'],
                        $r['Contact'],
                        $r['NextScheduledDate'],
                        $r['Reference'],
                        $r['Status'],
                    ], $preview)
                );
                if (count($results) > 20) {
                    $this->line('(preview: first 20 rows)');
                }
            }

            if (!$this->confirm('Continue to next group?', true)) {
                break;
            }
        }

        $this->info('Done.');
        return self::SUCCESS;
    }

    private function client(string $accessToken, string $tenantId)
    {
        return Http::withHeaders([
            'Authorization'  => 'Bearer ' . $accessToken,
            'xero-tenant-id' => $tenantId,
            'Accept'         => 'application/json',
        ])->timeout(60);
    }

    /**
     * Xero sometimes returns dates like:
     *  - "2026-03-20T00:00:00" (ISO)
     *  - "/Date(1775001600000+0000)/" (Xero JSON date)
     */
    private function parseXeroDate(string $value): ?Carbon
    {
        $value = trim($value);

        // Xero JSON date: /Date(1775001600000+0000)/
        if (preg_match('#^/Date\((\d+)([+-]\d{4})?\)/$#', $value, $m)) {
            $ms = (int) $m[1];
            return Carbon::createFromTimestampMs($ms)->utc();
        }

        // Try normal parsing (ISO etc.)
        try {
            return Carbon::parse($value);
        } catch (Throwable $e) {
            return null;
        }
    }

    private function writeCsv(string $groupName, Carbon $from, Carbon $to, array $rows): string
    {
        $safeGroup = preg_replace('/[^a-zA-Z0-9\- ]+/', '', $groupName) ?: 'group';
        $safeGroup = trim(str_replace(' ', '_', $safeGroup));

        $filename = sprintf(
            'xero/dd_%s_%s_to_%s_%s.csv',
            $safeGroup,
            $from->format('Ymd'),
            $to->format('Ymd'),
            now()->format('His')
        );

        Storage::makeDirectory('xero');
        $full = Storage::path($filename);

        $fh = fopen($full, 'w');
        fputcsv($fh, ['Group', 'RepeatingInvoiceID', 'Contact', 'NextScheduledDate', 'Reference', 'Status']);

        foreach ($rows as $r) {
            fputcsv($fh, [
                $r['Group'] ?? '',
                $r['RepeatingInvoiceID'] ?? '',
                $r['Contact'] ?? '',
                $r['NextScheduledDate'] ?? '',
                $r['Reference'] ?? '',
                $r['Status'] ?? '',
            ]);
        }

        fclose($fh);

        return $full;
    }
}