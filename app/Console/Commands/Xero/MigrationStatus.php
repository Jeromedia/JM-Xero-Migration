<?php

namespace App\Console\Commands\Xero;

use App\Models\XeroOrganisation;
use Illuminate\Console\Command;
use Illuminate\Support\Facades\Http;

class MigrationStatus extends Command
{
    protected $signature = 'xero:migration:status';
    protected $description = 'Show migration status across Xero entities';

    private $maxPages = 1000;

    public function handle(): int
    {
        $this->info('Xero Migration Status');
        $this->line('');

        $source = XeroOrganisation::where('role', 'source')->first();
        $test = XeroOrganisation::where('role', 'test')->first();
        $target = XeroOrganisation::where('role', 'target')->first();

        if (!$source || !$test || !$target) {
            $this->error('Missing organisation roles (source / test / target)');
            return Command::FAILURE;
        }

        /*
        |--------------------------------------------------------------------------
        | Entities + pagination support
        |--------------------------------------------------------------------------
        */

        $entities = [
            'Accounts' => false,
            'TaxRates' => false,
            'Contacts' => true,
            'Items' => false,
            'Invoices' => true,
        ];

        $rows = [];

        foreach ($entities as $entity => $paginated) {

            $this->line('');
            $this->info("Checking {$entity}");

            $sourceCount = $this->countEntity($source, $entity, 'SOURCE', $paginated);
            $testCount = $this->countEntity($test, $entity, 'TEST', $paginated);
            $targetCount = $this->countEntity($target, $entity, 'TARGET', $paginated);

            if ($sourceCount === 'ERR' || $testCount === 'ERR' || $targetCount === 'ERR') {

                $rows[] = [
                    $entity,
                    $sourceCount,
                    $testCount,
                    $targetCount,
                    '-',
                    'ERROR'
                ];

                continue;
            }

            $percent = $sourceCount > 0
                ? round(($testCount / $sourceCount) * 100)
                : 0;

            if ($testCount === $sourceCount) {
                $status = 'OK';
            } elseif ($testCount === 0) {
                $status = 'NOT STARTED';
            } else {
                $status = 'PARTIAL';
            }

            $rows[] = [
                $entity,
                $sourceCount,
                $testCount,
                $targetCount,
                "{$percent}%",
                $status
            ];
        }

        $this->line('');

        $this->table(
            ['Entity', 'Source', 'Test', 'Target', 'Progress', 'Status'],
            $rows
        );

        $this->line('');
        $this->info('Migration status complete.');

        return Command::SUCCESS;
    }

    /*
    |--------------------------------------------------------------------------
    | Safe Xero API Request (handles rate limits)
    |--------------------------------------------------------------------------
    */

    private function xeroRequest($org, $endpoint, $params = [])
    {
        while (true) {

            $response = Http::withToken($org->token->access_token)
                ->withHeaders([
                    'Xero-tenant-id' => $org->tenant_id,
                    'Accept' => 'application/json',
                ])
                ->get("https://api.xero.com/api.xro/2.0/{$endpoint}", $params);

            if ($response->status() === 429) {

                $retryAfter = $response->header('Retry-After') ?? 5;

                $this->warn("Rate limit reached. Waiting {$retryAfter}s...");

                sleep((int) $retryAfter);

                continue;
            }

            return $response;
        }
    }

    /*
    |--------------------------------------------------------------------------
    | Count entities
    |--------------------------------------------------------------------------
    */

    private function countEntity($org, $endpoint, $label, $paginated)
    {
        $page = 1;
        $total = 0;

        while (true) {

            if ($page > $this->maxPages) {
                $this->warn("Page safety limit reached ({$this->maxPages}).");
                break;
            }

            $params = $paginated ? ['page' => $page] : [];

            $response = $this->xeroRequest($org, $endpoint, $params);

            if (!$response->successful()) {

                $this->error("{$label} {$endpoint} fetch failed");
                return 'ERR';
            }

            $data = $response->json($endpoint);

            if (empty($data)) {
                break;
            }

            $count = count($data);
            $total += $count;

            $this->line("{$label} - page {$page} ({$total} records)");

            if (!$paginated) {
                break;
            }

            if ($count < 100) {
                break;
            }

            $page++;
        }

        return $total;
    }
}