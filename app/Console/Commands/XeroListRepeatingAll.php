<?php

namespace App\Console\Commands;

use App\Models\XeroOrganisation;
use App\Models\XeroToken;
use Carbon\Carbon;
use Illuminate\Console\Command;
use Illuminate\Support\Facades\Http;
use Illuminate\Support\Facades\Storage;
use Throwable;

class XeroListRepeatingAll extends Command
{
    protected $signature = 'xero:list-repeating-all {role : source|target}';
    protected $description = 'READ-ONLY: Export ALL repeating invoices into one CSV';

    public function handle(): int
    {
        try {
            return $this->runReport();
        } catch (Throwable $e) {
            $this->error('Fatal error: ' . $e->getMessage());
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

        $org = XeroOrganisation::where('role', $role)->first();
        if (!$org) {
            $this->error("No organisation set for role={$role}.");
            return self::FAILURE;
        }

        $token = XeroToken::find($org->xero_token_id);
        if (!$token || !$token->access_token) {
            $this->error('Token not found.');
            return self::FAILURE;
        }

        if (!$token->expires_at || $token->expires_at->isPast()) {
            $this->error('Access token expired. Run: php artisan xero:token-refresh');
            return self::FAILURE;
        }

        $tenantId = $org->tenant_id;

        $this->info('=== Xero Repeating Invoices (ALL) ===');
        $this->line("Role: {$role}");
        $this->line("Org: " . ($org->tenant_name ?? '(unknown)'));

        /*
        |--------------------------------------------------------------------------
        | Step A: Fetch Contacts (AccountNumber mapping)
        |--------------------------------------------------------------------------
        */
        $this->line('Fetching contacts...');

        $resp = $this->client($token->access_token, $tenantId)
            ->get('https://api.xero.com/api.xro/2.0/Contacts');

        if (!$resp->successful()) {
            $this->error('Failed fetching Contacts');
            return self::FAILURE;
        }

        $contacts = $resp->json()['Contacts'] ?? [];

        $contactAccountMap = [];

        foreach ($contacts as $c) {
            if (!empty($c['ContactID'])) {
                $contactAccountMap[$c['ContactID']] = $c['AccountNumber'] ?? '';
            }
        }

        $this->info('✓ Contacts loaded: ' . count($contactAccountMap));

        /*
        |--------------------------------------------------------------------------
        | Step B: Fetch Repeating Invoices
        |--------------------------------------------------------------------------
        */
        $this->line('Fetching repeating invoices...');

        $resp = $this->client($token->access_token, $tenantId)
            ->get('https://api.xero.com/api.xro/2.0/RepeatingInvoices');

        if (!$resp->successful()) {
            $this->error('Failed fetching RepeatingInvoices');
            return self::FAILURE;
        }

        $allRepeating = $resp->json()['RepeatingInvoices'] ?? [];

        $results = [];

        foreach ($allRepeating as $inv) {
            $cid = $inv['Contact']['ContactID'] ?? null;
            if (!$cid) continue;

            $next = $this->parseXeroDate($inv['Schedule']['NextScheduledDate'] ?? null);
            if (!$next) continue;

            // Use Xero totals
            $totalExGST  = $inv['SubTotal'] ?? 0;
            $totalGST    = $inv['TotalTax'] ?? 0;
            $totalIncGST = $inv['Total'] ?? ($totalExGST + $totalGST);

            $results[] = [
                'RepeatingInvoiceID' => $inv['RepeatingInvoiceID'] ?? '',
                'Contact'            => $inv['Contact']['Name'] ?? '',
                'AccountNumber'      => $contactAccountMap[$cid] ?? '',
                'NextScheduledDate'  => $next->toDateString(),
                'Reference'          => $inv['Reference'] ?? '',
                'Status'             => $inv['Status'] ?? '',
                'TotalExGST'         => round($totalExGST, 2),
                'TotalGST'           => round($totalGST, 2),
                'TotalIncGST'        => round($totalIncGST, 2),
            ];
        }

        $this->info('✓ Total invoices: ' . count($results));

        /*
        |--------------------------------------------------------------------------
        | Step C: Write CSV
        |--------------------------------------------------------------------------
        */
        $this->writeCsv($results);

        $this->info('Done.');
        return self::SUCCESS;
    }

    private function writeCsv(array $rows): void
    {
        $filename = 'xero/repeating_all_' . now()->format('Ymd_His') . '.csv';

        Storage::makeDirectory('xero');
        $full = Storage::path($filename);

        $fh = fopen($full, 'w');

        fputcsv($fh, [
            'RepeatingInvoiceID',
            'Contact',
            'AccountNumber',
            'NextScheduledDate',
            'Reference',
            'Status',
            'TotalExGST',
            'TotalGST',
            'TotalIncGST'
        ]);

        foreach ($rows as $r) {
            fputcsv($fh, $r);
        }

        fclose($fh);

        $this->info("✓ CSV saved: {$full}");
    }

    private function client(string $accessToken, string $tenantId)
    {
        return Http::withHeaders([
            'Authorization'  => 'Bearer ' . $accessToken,
            'xero-tenant-id' => $tenantId,
            'Accept'         => 'application/json',
        ])->timeout(60);
    }

    private function parseXeroDate(?string $value): ?Carbon
    {
        if (!$value) return null;

        if (preg_match('#^/Date\((\d+)#', $value, $m)) {
            return Carbon::createFromTimestampMs((int)$m[1])->utc();
        }

        try {
            return Carbon::parse($value);
        } catch (Throwable) {
            return null;
        }
    }
}