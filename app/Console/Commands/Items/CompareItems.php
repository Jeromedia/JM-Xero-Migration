<?php

namespace App\Console\Commands\Items;

use App\Models\XeroOrganisation;
use Illuminate\Console\Command;
use Illuminate\Http\Client\Response;
use Illuminate\Support\Collection;
use Illuminate\Support\Facades\Http;

class CompareItems extends Command
{
    protected $signature = 'xero:items:compare {--live : Compare SOURCE against TARGET instead of TEST}';
    protected $description = 'Compare items between SOURCE and TEST or TARGET';

    public function handle(): int
    {
        $destinationRole = $this->option('live') ? 'target' : 'test';

        $this->info('Starting items comparison...');
        $this->info('Source role: source');
        $this->info('Destination role: ' . $destinationRole);

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

        if (!$source->tenant_id || !$destination->tenant_id) {
            $this->error('Source or destination tenant ID not found.');
            return Command::FAILURE;
        }

        $this->info('Fetching SOURCE items...');
        $sourceItems = $this->fetchAllItems($source, 'SOURCE');

        $this->info('Fetching ' . strtoupper($destinationRole) . ' items...');
        $destinationItems = $this->fetchAllItems($destination, strtoupper($destinationRole));

        $sourceByCode = $this->indexByCode($sourceItems);
        $destinationByCode = $this->indexByCode($destinationItems);

        $matching = collect();
        $missingInDestination = collect();
        $extraInDestination = collect();
        $descriptionMismatches = collect();
        $salesAccountMismatches = collect();
        $purchaseAccountMismatches = collect();
        $inventoryAssetMismatches = collect();
        $statusDifferences = collect();

        foreach ($sourceByCode as $code => $sourceItem) {
            $destinationItem = $destinationByCode->get($code);

            if (!$destinationItem) {
                $missingInDestination->push($sourceItem);
                continue;
            }

            $matching->push($code);

            if (($sourceItem['Description'] ?? '') !== ($destinationItem['Description'] ?? '')) {
                $descriptionMismatches->push([
                    'Code' => $sourceItem['Code'] ?? '',
                    'SourceDescription' => $sourceItem['Description'] ?? '',
                    'DestinationDescription' => $destinationItem['Description'] ?? '',
                ]);
            }

            if (($sourceItem['SalesDetails']['AccountCode'] ?? '') !== ($destinationItem['SalesDetails']['AccountCode'] ?? '')) {
                $salesAccountMismatches->push([
                    'Code' => $sourceItem['Code'] ?? '',
                    'SourceSalesAccount' => $sourceItem['SalesDetails']['AccountCode'] ?? '',
                    'DestinationSalesAccount' => $destinationItem['SalesDetails']['AccountCode'] ?? '',
                ]);
            }

            if (($sourceItem['PurchaseDetails']['COGSAccountCode'] ?? '') !== ($destinationItem['PurchaseDetails']['COGSAccountCode'] ?? '')) {
                $purchaseAccountMismatches->push([
                    'Code' => $sourceItem['Code'] ?? '',
                    'SourcePurchaseAccount' => $sourceItem['PurchaseDetails']['COGSAccountCode'] ?? '',
                    'DestinationPurchaseAccount' => $destinationItem['PurchaseDetails']['COGSAccountCode'] ?? '',
                ]);
            }

            if (($sourceItem['InventoryAssetAccountCode'] ?? '') !== ($destinationItem['InventoryAssetAccountCode'] ?? '')) {
                $inventoryAssetMismatches->push([
                    'Code' => $sourceItem['Code'] ?? '',
                    'SourceInventoryAsset' => $sourceItem['InventoryAssetAccountCode'] ?? '',
                    'DestinationInventoryAsset' => $destinationItem['InventoryAssetAccountCode'] ?? '',
                ]);
            }

            if (($sourceItem['Status'] ?? '') !== ($destinationItem['Status'] ?? '')) {
                $statusDifferences->push([
                    'Code' => $sourceItem['Code'] ?? '',
                    'SourceStatus' => $sourceItem['Status'] ?? '',
                    'DestinationStatus' => $destinationItem['Status'] ?? '',
                ]);
            }
        }

        foreach ($destinationByCode as $code => $destinationItem) {
            if (!$sourceByCode->has($code)) {
                $extraInDestination->push($destinationItem);
            }
        }

        $this->newLine();
        $this->info('Summary');
        $this->line('SOURCE items: ' . $sourceItems->count());
        $this->line(strtoupper($destinationRole) . ' items: ' . $destinationItems->count());

        $this->newLine();
        $this->info('Comparison');
        $this->line('Matching by Code: ' . $matching->count());
        $this->line('Missing in ' . strtoupper($destinationRole) . ': ' . $missingInDestination->count());
        $this->line('Extra in ' . strtoupper($destinationRole) . ': ' . $extraInDestination->count());
        $this->line('Description mismatches: ' . $descriptionMismatches->count());
        $this->line('Sales account mismatches: ' . $salesAccountMismatches->count());
        $this->line('Purchase account mismatches: ' . $purchaseAccountMismatches->count());
        $this->line('Inventory asset mismatches: ' . $inventoryAssetMismatches->count());
        $this->line('Status differences: ' . $statusDifferences->count());

        if ($missingInDestination->isNotEmpty()) {
            $this->newLine();
            $this->warn('Missing items:');
            foreach ($missingInDestination as $item) {
                $this->line(' - ' . ($item['Code'] ?? ''));
            }
        }

        if ($extraInDestination->isNotEmpty()) {
            $this->newLine();
            $this->warn('Extra items:');
            foreach ($extraInDestination as $item) {
                $this->line(' - ' . ($item['Code'] ?? ''));
            }
        }

        if ($descriptionMismatches->isNotEmpty()) {
            $this->newLine();
            $this->warn('Description mismatches:');
            foreach ($descriptionMismatches as $row) {
                $this->line(sprintf(
                    ' - %s | SOURCE: %s | DESTINATION: %s',
                    $row['Code'],
                    $row['SourceDescription'],
                    $row['DestinationDescription']
                ));
            }
        }

        if ($salesAccountMismatches->isNotEmpty()) {
            $this->newLine();
            $this->warn('Sales account mismatches:');
            foreach ($salesAccountMismatches as $row) {
                $this->line(sprintf(
                    ' - %s | SOURCE: %s | DESTINATION: %s',
                    $row['Code'],
                    $row['SourceSalesAccount'],
                    $row['DestinationSalesAccount']
                ));
            }
        }

        if ($purchaseAccountMismatches->isNotEmpty()) {
            $this->newLine();
            $this->warn('Purchase account mismatches:');
            foreach ($purchaseAccountMismatches as $row) {
                $this->line(sprintf(
                    ' - %s | SOURCE: %s | DESTINATION: %s',
                    $row['Code'],
                    $row['SourcePurchaseAccount'],
                    $row['DestinationPurchaseAccount']
                ));
            }
        }

        if ($inventoryAssetMismatches->isNotEmpty()) {
            $this->newLine();
            $this->warn('Inventory asset mismatches:');
            foreach ($inventoryAssetMismatches as $row) {
                $this->line(sprintf(
                    ' - %s | SOURCE: %s | DESTINATION: %s',
                    $row['Code'],
                    $row['SourceInventoryAsset'],
                    $row['DestinationInventoryAsset']
                ));
            }
        }

        if ($statusDifferences->isNotEmpty()) {
            $this->newLine();
            $this->warn('Status differences:');
            foreach ($statusDifferences as $row) {
                $this->line(sprintf(
                    ' - %s | SOURCE: %s | DESTINATION: %s',
                    $row['Code'],
                    $row['SourceStatus'],
                    $row['DestinationStatus']
                ));
            }
        }

        $this->newLine();
        $this->info('Comparison complete.');

        return Command::SUCCESS;
    }

    private function fetchAllItems(XeroOrganisation $organisation, string $label): Collection
    {
        $response = $this->getItemsWithRetry($organisation, $label);

        return collect($response->json('Items', []));
    }

    private function getItemsWithRetry(XeroOrganisation $organisation, string $label): Response
    {
        $maxAttempts = 6;
        $attempt = 1;

        while ($attempt <= $maxAttempts) {
            $response = Http::withToken($organisation->token->access_token)
                ->withHeaders([
                    'Xero-tenant-id' => $organisation->tenant_id,
                    'Accept' => 'application/json',
                ])
                ->get('https://api.xero.com/api.xro/2.0/Items');

            if ($response->successful()) {
                return $response;
            }

            if ($response->status() === 429) {
                $retryAfter = (int) ($response->header('Retry-After') ?? 5);
                $retryAfter = $retryAfter > 0 ? $retryAfter : 5;

                $this->warn("Rate limited while fetching {$label} items. Waiting {$retryAfter} seconds before retry {$attempt}/{$maxAttempts}...");
                sleep($retryAfter);
                $attempt++;
                continue;
            }

            $this->error("Failed to fetch {$label} items.");
            $this->line('HTTP Status: ' . $response->status());
            $this->line($response->body());
            throw new \RuntimeException("Failed to fetch {$label} items.");
        }

        $this->error("Failed to fetch {$label} items after retries.");
        throw new \RuntimeException("Failed to fetch {$label} items after retries.");
    }

    private function indexByCode(Collection $items): Collection
    {
        return $items
            ->filter(fn ($item) => !empty($item['Code']))
            ->keyBy(fn ($item) => $this->normalizeCode($item['Code']));
    }

    private function normalizeCode(?string $value): ?string
    {
        if (!$value) {
            return null;
        }

        $normalized = strtolower(trim($value));

        return $normalized !== '' ? $normalized : null;
    }
}