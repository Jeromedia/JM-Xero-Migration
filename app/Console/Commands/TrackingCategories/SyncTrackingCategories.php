<?php

namespace App\Console\Commands\TrackingCategories;

use App\Models\XeroOrganisation;
use App\Services\Xero\XeroIdMapper;
use Illuminate\Console\Command;
use Illuminate\Support\Facades\Http;
use Illuminate\Support\Facades\Log;

class SyncTrackingCategories extends Command
{
    protected $signature = 'xero:tracking:sync {--live : Use target instead of test}';
    protected $description = 'Sync tracking categories from SOURCE to TEST or TARGET';

    private const ENTITY = 'tracking_category';
    private const OPTION_ENTITY = 'tracking_option';

    public function handle(): int
    {
        $destinationRole = $this->option('live') ? 'target' : 'test';

        $this->info('Starting tracking categories synchronization...');
        $this->info('Source role: source');
        $this->info('Destination role: ' . $destinationRole);

        $mapper = new XeroIdMapper();

        $mode = $this->choice(
            'Select migration mode',
            [
                'Test (1 tracking category only)',
                'Batch (all tracking categories)',
            ],
            0
        );

        $testMode = $mode === 'Test (1 tracking category only)';

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

        $this->info('Fetching SOURCE tracking categories...');

        $response = Http::withToken($source->token->access_token)
            ->withHeaders([
                'Xero-tenant-id' => $source->tenant_id,
                'Accept' => 'application/json',
            ])
            ->get('https://api.xero.com/api.xro/2.0/TrackingCategories');

        if (!$response->successful()) {
            $this->error('Failed to fetch tracking categories.');
            $this->line('HTTP Status: ' . $response->status());
            $this->line($response->body());
            return Command::FAILURE;
        }

        $trackingCategories = collect($response->json('TrackingCategories', []));

        if ($trackingCategories->isEmpty()) {
            $this->info('No tracking categories found.');
            return Command::SUCCESS;
        }

        if ($testMode) {
            $trackingCategories = $trackingCategories->take(1);
        } else {
            $this->info('Tracking categories to process: ' . $trackingCategories->count());

            if (!$this->confirm('Send all tracking categories now?', false)) {
                return Command::SUCCESS;
            }
        }

        foreach ($trackingCategories as $trackingCategory) {
            $sourceTrackingCategoryId = $trackingCategory['TrackingCategoryID'];
            $trackingCategoryName = $trackingCategory['Name'];

            $existingCategoryId = $destinationRole === 'test'
                ? $mapper->getTestId(self::ENTITY, $sourceTrackingCategoryId)
                : $mapper->getTargetId(self::ENTITY, $sourceTrackingCategoryId);

            if ($existingCategoryId) {
                $destinationTrackingCategoryId = $existingCategoryId;
                $this->info("Mapped already → Reusing category: {$trackingCategoryName} (ID: {$destinationTrackingCategoryId})");
            } else {
                $cleanTrackingCategory = collect($trackingCategory)
                    ->except([
                        'TrackingCategoryID',
                        'HasValidationErrors',
                        'Status',
                        'Options',
                    ])
                    ->filter(function ($value) {
                        return $value !== null && $value !== '';
                    })
                    ->toArray();

                $categoryPayload = $cleanTrackingCategory;

                if ($testMode) {
                    $this->warn("Next tracking category: {$trackingCategoryName}");
                    $this->line(json_encode($categoryPayload, JSON_PRETTY_PRINT));

                    if (!$this->confirm('Send this tracking category now?', false)) {
                        return Command::SUCCESS;
                    }
                } else {
                    $this->info("Processing category: {$trackingCategoryName}");
                }

                $putCategory = Http::withToken($destination->token->access_token)
                    ->withHeaders([
                        'Xero-tenant-id' => $destination->tenant_id,
                        'Accept' => 'application/json',
                        'Content-Type' => 'application/json',
                    ])
                    ->put('https://api.xero.com/api.xro/2.0/TrackingCategories', $categoryPayload);

                if (!$putCategory->successful()) {
                    $this->error("Failed to add tracking category {$trackingCategoryName}");
                    $this->line('HTTP Status: ' . $putCategory->status());
                    $this->line($putCategory->body());

                    Log::error('Xero tracking category migration failed', [
                        'category_name' => $trackingCategoryName,
                        'payload' => $categoryPayload,
                        'status' => $putCategory->status(),
                        'response' => $putCategory->body(),
                    ]);

                    return Command::FAILURE;
                }

                $createdCategory = $putCategory->json('TrackingCategories.0');
                $destinationTrackingCategoryId = $createdCategory['TrackingCategoryID'] ?? null;

                if (!$destinationTrackingCategoryId) {
                    $this->error('No destination category ID returned. Aborting.');
                    return Command::FAILURE;
                }

                if ($destinationRole === 'test') {
                    $mapper->storeTest(
                        self::ENTITY,
                        $sourceTrackingCategoryId,
                        $destinationTrackingCategoryId,
                        $source->tenant_id,
                        $destination->tenant_id,
                        $trackingCategoryName
                    );

                    $storedCategoryId = $mapper->getTestId(self::ENTITY, $sourceTrackingCategoryId);
                } else {
                    $mapper->storeTarget(
                        self::ENTITY,
                        $sourceTrackingCategoryId,
                        $destinationTrackingCategoryId,
                        $source->tenant_id,
                        $destination->tenant_id,
                        $trackingCategoryName
                    );

                    $storedCategoryId = $mapper->getTargetId(self::ENTITY, $sourceTrackingCategoryId);
                }

                if (!$storedCategoryId || $storedCategoryId !== $destinationTrackingCategoryId) {
                    $this->error("Category mapping verification failed for {$trackingCategoryName}. Aborting.");
                    return Command::FAILURE;
                }

                $this->info("Added + mapped category {$trackingCategoryName} → {$destinationTrackingCategoryId}");
            }

            $sourceOptions = collect($trackingCategory['Options'] ?? []);

            foreach ($sourceOptions as $sourceOption) {
                $sourceOptionId = $sourceOption['TrackingOptionID'] ?? null;
                $sourceOptionName = $sourceOption['Name'] ?? null;

                if (!$sourceOptionId || !$sourceOptionName) {
                    continue;
                }

                $optionSourceKey = $this->buildOptionSourceKey($sourceTrackingCategoryId, $sourceOptionId);

                $existingOptionId = $destinationRole === 'test'
                    ? $mapper->getTestId(self::OPTION_ENTITY, $optionSourceKey)
                    : $mapper->getTargetId(self::OPTION_ENTITY, $optionSourceKey);

                if ($existingOptionId) {
                    $this->info("Mapped already → Skipping option: {$sourceOptionName} (ID: {$existingOptionId})");
                    continue;
                }

                $cleanOption = collect($sourceOption)
                    ->except([
                        'TrackingOptionID',
                        'HasValidationErrors',
                        'Status',
                    ])
                    ->filter(function ($value) {
                        return $value !== null && $value !== '';
                    })
                    ->toArray();

                $optionPayload = $cleanOption;

                if ($testMode) {
                    $this->line('Option payload:');
                    $this->line(json_encode($optionPayload, JSON_PRETTY_PRINT));
                } else {
                    $this->info("Creating option: {$sourceOptionName}");
                }

                $putOption = Http::withToken($destination->token->access_token)
                    ->withHeaders([
                        'Xero-tenant-id' => $destination->tenant_id,
                        'Accept' => 'application/json',
                        'Content-Type' => 'application/json',
                    ])
                    ->put(
                        "https://api.xero.com/api.xro/2.0/TrackingCategories/{$destinationTrackingCategoryId}/Options",
                        $optionPayload
                    );

                if (!$putOption->successful()) {
                    $this->error("Failed to add option {$sourceOptionName} in category {$trackingCategoryName}");
                    $this->line('HTTP Status: ' . $putOption->status());
                    $this->line($putOption->body());

                    Log::error('Xero tracking option migration failed', [
                        'category_name' => $trackingCategoryName,
                        'category_id' => $destinationTrackingCategoryId,
                        'option_name' => $sourceOptionName,
                        'payload' => $optionPayload,
                        'status' => $putOption->status(),
                        'response' => $putOption->body(),
                    ]);

                    return Command::FAILURE;
                }

                $destinationOptionId = $putOption->json('Options.0.TrackingOptionID')
                    ?? $putOption->json('TrackingOptions.0.TrackingOptionID')
                    ?? $putOption->json('TrackingCategory.Options.0.TrackingOptionID')
                    ?? null;

                if (!$destinationOptionId) {
                    $fetchCategory = Http::withToken($destination->token->access_token)
                        ->withHeaders([
                            'Xero-tenant-id' => $destination->tenant_id,
                            'Accept' => 'application/json',
                        ])
                        ->get('https://api.xero.com/api.xro/2.0/TrackingCategories');

                    if ($fetchCategory->successful()) {
                        $destinationCategories = collect($fetchCategory->json('TrackingCategories', []));
                        $matchedCategory = $destinationCategories->firstWhere('TrackingCategoryID', $destinationTrackingCategoryId);

                        $matchedOption = collect($matchedCategory['Options'] ?? [])->first(function (array $option) use ($sourceOptionName) {
                            return ($option['Name'] ?? null) === $sourceOptionName;
                        });

                        $destinationOptionId = $matchedOption['TrackingOptionID'] ?? null;
                    }
                }

                if (!$destinationOptionId) {
                    $this->error("No destination option ID returned for option {$sourceOptionName}. Aborting.");
                    return Command::FAILURE;
                }

                if ($destinationRole === 'test') {
                    $mapper->storeTest(
                        self::OPTION_ENTITY,
                        $optionSourceKey,
                        $destinationOptionId,
                        $source->tenant_id,
                        $destination->tenant_id,
                        $sourceOptionName
                    );

                    $storedOptionId = $mapper->getTestId(self::OPTION_ENTITY, $optionSourceKey);
                } else {
                    $mapper->storeTarget(
                        self::OPTION_ENTITY,
                        $optionSourceKey,
                        $destinationOptionId,
                        $source->tenant_id,
                        $destination->tenant_id,
                        $sourceOptionName
                    );

                    $storedOptionId = $mapper->getTargetId(self::OPTION_ENTITY, $optionSourceKey);
                }

                if (!$storedOptionId || $storedOptionId !== $destinationOptionId) {
                    $this->error("Option mapping verification failed for {$sourceOptionName}. Aborting.");
                    return Command::FAILURE;
                }

                $this->info("Added + mapped option {$sourceOptionName} → {$destinationOptionId}");

                usleep(500000);
            }

            if ($testMode) {
                return Command::SUCCESS;
            }

            usleep(500000);
        }

        $this->info('Finished.');
        return Command::SUCCESS;
    }

    private function buildOptionSourceKey(string $trackingCategoryId, string $trackingOptionId): string
    {
        return $trackingCategoryId . ':' . $trackingOptionId;
    }
}