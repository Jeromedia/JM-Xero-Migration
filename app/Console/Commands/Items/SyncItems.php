<?php

namespace App\Console\Commands\Items;

use App\Models\XeroOrganisation;
use App\Services\Xero\XeroIdMapper;
use Illuminate\Console\Command;
use Illuminate\Support\Facades\Http;
use Illuminate\Support\Facades\Log;

class SyncItems extends Command
{
    protected $signature = 'xero:items:sync {--live : Use target instead of test}';
    protected $description = 'Sync items from SOURCE to TEST or TARGET';

    public function handle(): int
    {
        $destinationRole = $this->option('live') ? 'target' : 'test';

        $this->info('Starting items synchronization...');
        $this->info('Source role: source');
        $this->info('Destination role: ' . $destinationRole);

        $mapper = new XeroIdMapper();

        $mode = $this->choice(
            'Select migration mode',
            [
                'Test (1 item only)',
                'Batch (100 items per page)'
            ],
            0
        );

        $testMode = $mode === 'Test (1 item only)';

        $source = XeroOrganisation::where('role', 'source')->first();
        $destination = XeroOrganisation::where('role', $destinationRole)->first();

        $page = 1;

        while (true) {

            $this->info("Fetching SOURCE items (page {$page})...");

            $response = Http::withToken($source->token->access_token)
                ->withHeaders([
                    'Xero-tenant-id' => $source->tenant_id,
                    'Accept' => 'application/json',
                ])
                ->get(
                    'https://api.xero.com/api.xro/2.0/Items',
                    ['page' => $page]
                );

            if (!$response->successful()) {
                $this->error('Failed to fetch items.');
                return Command::FAILURE;
            }

            $items = collect($response->json('Items'));

            if ($items->isEmpty()) {
                break;
            }

            if ($testMode) {
                $items = $items->take(1);
            }

            $processedOnPage = 0;

            foreach ($items as $item) {

                $sourceItemId = $item['ItemID'];

                $existing = $destinationRole === 'test'
                    ? $mapper->getTestId('item', $sourceItemId)
                    : $mapper->getTargetId('item', $sourceItemId);

                if ($existing) {
                    $this->info("Already migrated. Skipping: " . ($item['Code'] ?? 'Unnamed Item'));
                    continue;
                }

                $cleanItem = collect($item)
                    ->except([
                        'ItemID',
                        'HasAttachments',
                        'HasValidationErrors',
                        'UpdatedDateUTC'
                    ])
                    ->filter()
                    ->toArray();

                /*
                |--------------------------------------------------------------------------
                | Strict Account Validation
                |--------------------------------------------------------------------------
                */

                if (isset($cleanItem['SalesDetails']['AccountCode'])) {

                    $accountCode = $cleanItem['SalesDetails']['AccountCode'];

                    $check = Http::withToken($destination->token->access_token)
                        ->withHeaders([
                            'Xero-tenant-id' => $destination->tenant_id,
                            'Accept' => 'application/json',
                        ])
                        ->get(
                            'https://api.xero.com/api.xro/2.0/Accounts',
                            ['where' => 'Code=="' . $accountCode . '"']
                        );

                    if (!$check->successful()) {
                        $this->error("Failed to validate account {$accountCode}");
                        return Command::FAILURE;
                    }

                    $accountExists = collect($check->json('Accounts'))->isNotEmpty();

                    if (!$accountExists) {

                        $this->error("Account {$accountCode} not found in destination.");
                        $this->error("Item '{$item['Code']}' cannot be migrated.");
                        $this->error("Please migrate the Chart of Accounts first.");

                        return Command::FAILURE;
                    }
                }

                $payload = [
                    'Items' => [
                        $cleanItem
                    ]
                ];

                $jsonPayload = json_encode(
                    $payload,
                    JSON_PRETTY_PRINT | JSON_UNESCAPED_SLASHES
                );

                $this->warn("Next item: " . ($item['Code'] ?? 'Unnamed Item'));
                $this->line($jsonPayload);

                if (!$this->confirm('Send this item now?', false)) {
                    return Command::SUCCESS;
                }

                $post = Http::withToken($destination->token->access_token)
                    ->withHeaders([
                        'Xero-tenant-id' => $destination->tenant_id,
                        'Accept' => 'application/json',
                        'Content-Type' => 'application/json'
                    ])
                    ->post(
                        'https://api.xero.com/api.xro/2.0/Items',
                        $payload
                    );

                if (!$post->successful()) {

                    $this->error("Failed to add " . ($item['Code'] ?? 'Unnamed Item'));

                    Log::error('Xero item migration failed', [
                        'payload' => $payload,
                        'response' => $post->body()
                    ]);

                    continue;
                }

                $destinationId = $post->json('Items.0.ItemID');

                if ($destinationRole === 'test') {

                    $mapper->storeTest(
                        'item',
                        $sourceItemId,
                        $destinationId,
                        $source->tenant_id,
                        $destination->tenant_id,
                        $item['Code'] ?? null
                    );

                } else {

                    $mapper->storeTarget(
                        'item',
                        $sourceItemId,
                        $destinationId,
                        $source->tenant_id,
                        $destination->tenant_id
                    );
                }

                $this->info("Added " . ($item['Code'] ?? 'Unnamed Item'));

                $processedOnPage++;

                if ($testMode) {
                    return Command::SUCCESS;
                }

                usleep(500000);
            }

            /*
            |--------------------------------------------------------------------------
            | Stop pagination if nothing processed on this page
            |--------------------------------------------------------------------------
            */

            if ($processedOnPage === 0) {
                $this->info("No new items on page {$page}. Stopping pagination.");
                break;
            }

            $page++;
        }

        $this->info('Finished.');
        return Command::SUCCESS;
    }
}