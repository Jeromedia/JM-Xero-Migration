<?php

namespace App\Console\Commands\Items;

use App\Models\XeroOrganisation;
use App\Services\Xero\XeroIdMapper;
use Illuminate\Console\Command;
use Illuminate\Http\Client\Response;
use Illuminate\Support\Collection;
use Illuminate\Support\Facades\Http;
use Illuminate\Support\Facades\Log;

class SyncItems extends Command
{
    protected $signature = 'xero:items:sync
                            {--live : Use target instead of test}
                            {--code= : Sync only one item by exact Code}
                            {--source-id= : Sync only one item by source ItemID}
                            {--first : Sync only the first source item}';

    protected $description = 'Sync items from SOURCE to TEST or TARGET with mapping validation and recovery';

    private const ENTITY = 'item';

    public function handle(): int
    {
        $destinationRole = $this->option('live') ? 'target' : 'test';

        $this->info('Starting items sync (VALIDATED SMART MODE)...');
        $this->info('Source role: source');
        $this->info('Destination role: ' . $destinationRole);

        if ($this->option('code')) {
            $this->info('Filter mode: exact code = ' . $this->option('code'));
        }

        if ($this->option('source-id')) {
            $this->info('Filter mode: source ItemID = ' . $this->option('source-id'));
        }

        if ($this->option('first')) {
            $this->info('Filter mode: first source item only');
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

        if (!$source->tenant_id || !$destination->tenant_id) {
            $this->error('Source or destination tenant ID not found.');
            return Command::FAILURE;
        }

        $this->info('Loading destination items index...');
        $destinationItems = $this->fetchAllItems($destination, strtoupper($destinationRole));
        [$destinationById, $destinationByCode] = $this->buildDestinationIndexes($destinationItems);

        $this->info('Fetching SOURCE items...');
        $sourceItems = $this->fetchAllItems($source, 'SOURCE');
        $sourceItems = $this->filterSourceItems($sourceItems);

        if ($sourceItems->isEmpty()) {
            $this->info('No items found.');
            return Command::SUCCESS;
        }

        if ($this->option('first')) {
            $sourceItems = collect([$sourceItems->first()]);
        }

        $processed = 0;
        $mappedValid = 0;
        $mappedInvalid = 0;
        $recovered = 0;
        $created = 0;

        foreach ($sourceItems as $item) {
            $processed++;

            $sourceItemId = $item['ItemID'] ?? null;
            $code = $item['Code'] ?? null;

            if (!$sourceItemId || !$code) {
                $this->error('Invalid source item.');
                $this->line(json_encode($item, JSON_PRETTY_PRINT | JSON_UNESCAPED_SLASHES));
                return Command::FAILURE;
            }

            $mappedId = $destinationRole === 'test'
                ? $mapper->getTestId(self::ENTITY, $sourceItemId)
                : $mapper->getTargetId(self::ENTITY, $sourceItemId);

            if ($mappedId) {
                $mappedItem = $destinationById->get($mappedId);

                if ($mappedItem && $this->isReasonableMappedMatch($item, $mappedItem)) {
                    $mappedValid++;
                    $this->info("Mapped already → Valid: {$code} ({$mappedId})");
                    continue;
                }

                $mappedInvalid++;

                Log::warning('Mapped item invalid', [
                    'source_item_id' => $sourceItemId,
                    'mapped_target_id' => $mappedId,
                    'source_code' => $item['Code'] ?? null,
                    'mapped_code' => $mappedItem['Code'] ?? null,
                    'source_sales_account' => $item['SalesDetails']['AccountCode'] ?? null,
                    'mapped_sales_account' => $mappedItem['SalesDetails']['AccountCode'] ?? null,
                    'source_purchase_account' => $item['PurchaseDetails']['COGSAccountCode'] ?? null,
                    'mapped_purchase_account' => $mappedItem['PurchaseDetails']['COGSAccountCode'] ?? null,
                    'source_inventory_asset' => $item['InventoryAssetAccountCode'] ?? null,
                    'mapped_inventory_asset' => $mappedItem['InventoryAssetAccountCode'] ?? null,
                ]);
            }

            $matchedDestination = $this->findDestinationMatch($item, $destinationByCode);

            if ($matchedDestination) {
                $this->storeMapping(
                    $mapper,
                    $destinationRole,
                    $source,
                    $destination,
                    $sourceItemId,
                    $matchedDestination['ItemID'],
                    $code
                );

                $verifiedId = $destinationRole === 'test'
                    ? $mapper->getTestId(self::ENTITY, $sourceItemId)
                    : $mapper->getTargetId(self::ENTITY, $sourceItemId);

                if (!$verifiedId || $verifiedId !== $matchedDestination['ItemID']) {
                    $this->error("Mapping verification failed for recovered item: {$code}");
                    return Command::FAILURE;
                }

                $recovered++;
                $this->info("Recovered + mapped: {$code} ({$matchedDestination['ItemID']})");
                continue;
            }

            $cleanItem = $this->sanitizeItem($item);

            if (empty($cleanItem['Code'])) {
                $this->error("Sanitized item has no Code: {$code}");
                $this->line(json_encode($item, JSON_PRETTY_PRINT | JSON_UNESCAPED_SLASHES));
                return Command::FAILURE;
            }

            $validationError = $this->validateReferencedAccounts($destination, $cleanItem, $code);

            if ($validationError !== null) {
                $this->error($validationError);
                return Command::FAILURE;
            }

            $payload = [
                'Items' => [$cleanItem],
            ];

            $post = $this->postItemsWithRetry($destination, $payload, $code);
            $destinationId = $post->json('Items.0.ItemID');

            if ($post->successful() && $destinationId) {
                $this->storeMapping(
                    $mapper,
                    $destinationRole,
                    $source,
                    $destination,
                    $sourceItemId,
                    $destinationId,
                    $code
                );

                $verifiedId = $destinationRole === 'test'
                    ? $mapper->getTestId(self::ENTITY, $sourceItemId)
                    : $mapper->getTargetId(self::ENTITY, $sourceItemId);

                if (!$verifiedId || $verifiedId !== $destinationId) {
                    $this->error("Mapping verification failed for created item: {$code}");
                    return Command::FAILURE;
                }

                $createdItem = $post->json('Items.0');
                if (is_array($createdItem)) {
                    $destinationItems->push($createdItem);
                    [$destinationById, $destinationByCode] = $this->buildDestinationIndexes($destinationItems);
                }

                $created++;
                $this->info("Created + mapped: {$code} ({$destinationId})");
                continue;
            }

            $this->warn("Create not completed -> refreshing destination index and retrying recovery: {$code}");

            $destinationItems = $this->fetchAllItems($destination, strtoupper($destinationRole));
            [$destinationById, $destinationByCode] = $this->buildDestinationIndexes($destinationItems);

            $matchedDestination = $this->findDestinationMatch($item, $destinationByCode);

            if ($matchedDestination) {
                $this->storeMapping(
                    $mapper,
                    $destinationRole,
                    $source,
                    $destination,
                    $sourceItemId,
                    $matchedDestination['ItemID'],
                    $code
                );

                $verifiedId = $destinationRole === 'test'
                    ? $mapper->getTestId(self::ENTITY, $sourceItemId)
                    : $mapper->getTargetId(self::ENTITY, $sourceItemId);

                if (!$verifiedId || $verifiedId !== $matchedDestination['ItemID']) {
                    $this->error("Mapping verification failed after recovery for: {$code}");
                    return Command::FAILURE;
                }

                $recovered++;
                $this->info("Recovered after refresh + mapped: {$code} ({$matchedDestination['ItemID']})");
                continue;
            }

            $this->error("Unable to create or recover item: {$code}");
            $this->line('HTTP Status: ' . $post->status());
            $this->line($post->body());
            $this->line(json_encode($payload, JSON_PRETTY_PRINT | JSON_UNESCAPED_SLASHES));

            Log::error('Xero item migration failed', [
                'source_item_id' => $sourceItemId,
                'code' => $code,
                'payload' => $payload,
                'status' => $post->status(),
                'response' => $post->body(),
            ]);

            return Command::FAILURE;
        }

        $this->newLine();
        $this->info('Done.');
        $this->line("Processed: {$processed}");
        $this->line("Valid mapped: {$mappedValid}");
        $this->line("Invalid mapped found: {$mappedInvalid}");
        $this->line("Recovered + mapped: {$recovered}");
        $this->line("Created + mapped: {$created}");

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

    private function postItemsWithRetry(XeroOrganisation $organisation, array $payload, string $code): Response
    {
        $maxAttempts = 6;
        $attempt = 1;

        while ($attempt <= $maxAttempts) {
            $response = Http::withToken($organisation->token->access_token)
                ->withHeaders([
                    'Xero-tenant-id' => $organisation->tenant_id,
                    'Accept' => 'application/json',
                    'Content-Type' => 'application/json',
                ])
                ->post('https://api.xero.com/api.xro/2.0/Items', $payload);

            if ($response->successful()) {
                return $response;
            }

            if ($response->status() === 429) {
                $retryAfter = (int) ($response->header('Retry-After') ?? 5);
                $retryAfter = $retryAfter > 0 ? $retryAfter : 5;

                $this->warn("Rate limited while creating item {$code}. Waiting {$retryAfter} seconds before retry {$attempt}/{$maxAttempts}...");
                sleep($retryAfter);
                $attempt++;
                continue;
            }

            return $response;
        }

        $this->error("Rate limited while creating item {$code} after retries.");
        throw new \RuntimeException("Rate limited while creating item {$code} after retries.");
    }

    private function buildDestinationIndexes(Collection $items): array
    {
        $byId = $items
            ->filter(fn ($item) => !empty($item['ItemID']))
            ->keyBy('ItemID');

        $byCode = $items
            ->filter(fn ($item) => !empty($item['Code']))
            ->groupBy(fn ($item) => $this->normalizeCode($item['Code']));

        return [$byId, $byCode];
    }

    private function filterSourceItems(Collection $items): Collection
    {
        if ($this->option('source-id')) {
            $sourceId = trim((string) $this->option('source-id'));

            return $items
                ->filter(fn (array $item) => ($item['ItemID'] ?? '') === $sourceId)
                ->values();
        }

        if ($this->option('code')) {
            $code = trim((string) $this->option('code'));

            return $items
                ->filter(fn (array $item) => trim((string) ($item['Code'] ?? '')) === $code)
                ->values();
        }

        return $items->values();
    }

    private function findDestinationMatch(array $sourceItem, Collection $destinationByCode): ?array
    {
        $codeKey = $this->normalizeCode($sourceItem['Code'] ?? null);

        if (!$codeKey) {
            return null;
        }

        $matches = $destinationByCode->get($codeKey);

        if ($matches && $matches->count() === 1) {
            return $matches->first();
        }

        return null;
    }

    private function isReasonableMappedMatch(array $sourceItem, array $destinationItem): bool
    {
        $sourceCode = $this->normalizeCode($sourceItem['Code'] ?? null);
        $destinationCode = $this->normalizeCode($destinationItem['Code'] ?? null);

        if (!$sourceCode || !$destinationCode || $sourceCode !== $destinationCode) {
            return false;
        }

        $sourceSalesAccount = trim((string) ($sourceItem['SalesDetails']['AccountCode'] ?? ''));
        $destinationSalesAccount = trim((string) ($destinationItem['SalesDetails']['AccountCode'] ?? ''));

        if ($sourceSalesAccount !== '' && $destinationSalesAccount !== '' && $sourceSalesAccount !== $destinationSalesAccount) {
            return false;
        }

        $sourcePurchaseAccount = trim((string) ($sourceItem['PurchaseDetails']['COGSAccountCode'] ?? ''));
        $destinationPurchaseAccount = trim((string) ($destinationItem['PurchaseDetails']['COGSAccountCode'] ?? ''));

        if ($sourcePurchaseAccount !== '' && $destinationPurchaseAccount !== '' && $sourcePurchaseAccount !== $destinationPurchaseAccount) {
            return false;
        }

        $sourceInventoryAsset = trim((string) ($sourceItem['InventoryAssetAccountCode'] ?? ''));
        $destinationInventoryAsset = trim((string) ($destinationItem['InventoryAssetAccountCode'] ?? ''));

        if ($sourceInventoryAsset !== '' && $destinationInventoryAsset !== '' && $sourceInventoryAsset !== $destinationInventoryAsset) {
            return false;
        }

        return true;
    }

    private function sanitizeItem(array $item): array
    {
        return collect($item)
            ->except([
                'ItemID',
                'HasAttachments',
                'HasValidationErrors',
                'UpdatedDateUTC',
                'ValidationErrors',
            ])
            ->map(fn ($value) => $this->sanitizeValue($value))
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

    private function validateReferencedAccounts(XeroOrganisation $destination, array $cleanItem, string $code): ?string
    {
        $accountCodes = collect([
            $cleanItem['SalesDetails']['AccountCode'] ?? null,
            $cleanItem['PurchaseDetails']['COGSAccountCode'] ?? null,
            $cleanItem['InventoryAssetAccountCode'] ?? null,
        ])->filter()->unique()->values();

        foreach ($accountCodes as $accountCode) {
            $check = $this->getAccountsByCodeWithRetry($destination, $accountCode);

            if (!$check->successful()) {
                return "Failed to validate account {$accountCode} for item {$code}";
            }

            $accountExists = collect($check->json('Accounts', []))->isNotEmpty();

            if (!$accountExists) {
                return "Account {$accountCode} not found in destination. Item {$code} cannot be migrated. Please migrate the Chart of Accounts first.";
            }
        }

        return null;
    }

    private function getAccountsByCodeWithRetry(XeroOrganisation $organisation, string $accountCode): Response
    {
        $maxAttempts = 6;
        $attempt = 1;

        while ($attempt <= $maxAttempts) {
            $response = Http::withToken($organisation->token->access_token)
                ->withHeaders([
                    'Xero-tenant-id' => $organisation->tenant_id,
                    'Accept' => 'application/json',
                ])
                ->get('https://api.xero.com/api.xro/2.0/Accounts', [
                    'where' => 'Code=="' . $accountCode . '"',
                ]);

            if ($response->successful()) {
                return $response;
            }

            if ($response->status() === 429) {
                $retryAfter = (int) ($response->header('Retry-After') ?? 5);
                $retryAfter = $retryAfter > 0 ? $retryAfter : 5;

                $this->warn("Rate limited while validating account {$accountCode}. Waiting {$retryAfter} seconds before retry {$attempt}/{$maxAttempts}...");
                sleep($retryAfter);
                $attempt++;
                continue;
            }

            return $response;
        }

        $this->error("Rate limited while validating account {$accountCode} after retries.");
        throw new \RuntimeException("Rate limited while validating account {$accountCode} after retries.");
    }

    private function normalizeCode(?string $value): ?string
    {
        if (!$value) {
            return null;
        }

        $normalized = strtolower(trim($value));
        return $normalized !== '' ? $normalized : null;
    }

    private function storeMapping(
        XeroIdMapper $mapper,
        string $destinationRole,
        XeroOrganisation $source,
        XeroOrganisation $destination,
        string $sourceId,
        string $destinationId,
        ?string $code
    ): void {
        if ($destinationRole === 'test') {
            $mapper->storeTest(
                self::ENTITY,
                $sourceId,
                $destinationId,
                $source->tenant_id,
                $destination->tenant_id,
                $code
            );
            return;
        }

        $mapper->storeTarget(
            self::ENTITY,
            $sourceId,
            $destinationId,
            $source->tenant_id,
            $destination->tenant_id,
            $code
        );
    }
}