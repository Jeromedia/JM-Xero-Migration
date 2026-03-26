<?php

namespace App\Console\Commands\TrackingCategories;

use App\Models\XeroOrganisation;
use App\Services\Xero\XeroIdMapper;
use Illuminate\Console\Command;
use Illuminate\Http\Client\Response;
use Illuminate\Support\Collection;
use Illuminate\Support\Facades\Http;
use Illuminate\Support\Facades\Log;

class SyncTrackingCategories extends Command
{
    protected $signature = 'xero:tracking:sync
                            {--live : Use target instead of test}
                            {--name= : Sync only one tracking category by exact Name}
                            {--source-id= : Sync only one tracking category by source TrackingCategoryID}
                            {--first : Sync only the first source tracking category}';

    protected $description = 'Sync tracking categories and tracking options from SOURCE to TEST or TARGET with mapping validation and recovery';

    private const ENTITY = 'tracking_category';
    private const OPTION_ENTITY = 'tracking_option';

    public function handle(): int
    {
        $destinationRole = $this->option('live') ? 'target' : 'test';

        $this->info('Starting tracking categories sync (VALIDATED SMART MODE)...');
        $this->info('Source role: source');
        $this->info('Destination role: ' . $destinationRole);

        if ($this->option('name')) {
            $this->info('Filter mode: exact name = ' . $this->option('name'));
        }

        if ($this->option('source-id')) {
            $this->info('Filter mode: source TrackingCategoryID = ' . $this->option('source-id'));
        }

        if ($this->option('first')) {
            $this->info('Filter mode: first source tracking category only');
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

        $this->info('Loading destination tracking categories index...');
        $destinationCategories = $this->fetchTrackingCategories($destination, strtoupper($destinationRole));
        [$destinationById, $destinationByName] = $this->buildDestinationIndexes($destinationCategories);

        $this->info('Fetching SOURCE tracking categories...');
        $sourceCategories = $this->fetchTrackingCategories($source, 'SOURCE');
        $sourceCategories = $this->filterSourceCategories($sourceCategories);

        if ($sourceCategories->isEmpty()) {
            $this->info('No tracking categories found.');
            return Command::SUCCESS;
        }

        if ($this->option('first')) {
            $sourceCategories = collect([$sourceCategories->first()]);
        }

        $processed = 0;
        $mappedValid = 0;
        $mappedInvalid = 0;
        $recovered = 0;
        $created = 0;
        $optionsMappedValid = 0;
        $optionsRecovered = 0;
        $optionsCreated = 0;

        foreach ($sourceCategories as $trackingCategory) {
            $processed++;

            $sourceTrackingCategoryId = $trackingCategory['TrackingCategoryID'] ?? null;
            $trackingCategoryName = $trackingCategory['Name'] ?? null;

            if (!$sourceTrackingCategoryId || !$trackingCategoryName) {
                $this->error('Invalid source tracking category.');
                $this->line(json_encode($trackingCategory, JSON_PRETTY_PRINT | JSON_UNESCAPED_SLASHES));
                return Command::FAILURE;
            }

            $mappedCategoryId = $destinationRole === 'test'
                ? $mapper->getTestId(self::ENTITY, $sourceTrackingCategoryId)
                : $mapper->getTargetId(self::ENTITY, $sourceTrackingCategoryId);

            $destinationTrackingCategory = null;
            $destinationTrackingCategoryId = null;

            if ($mappedCategoryId) {
                $mappedCategory = $destinationById->get($mappedCategoryId);

                if ($mappedCategory && $this->isReasonableMappedCategoryMatch($trackingCategory, $mappedCategory)) {
                    $destinationTrackingCategory = $mappedCategory;
                    $destinationTrackingCategoryId = $mappedCategoryId;
                    $mappedValid++;
                    $this->info("Mapped already → Valid category: {$trackingCategoryName} ({$destinationTrackingCategoryId})");
                } else {
                    $mappedInvalid++;
                }
            }

            if (!$destinationTrackingCategory) {
                $matchedDestinationCategory = $this->findDestinationCategoryMatch($trackingCategory, $destinationByName);

                if ($matchedDestinationCategory) {
                    $destinationTrackingCategory = $matchedDestinationCategory;
                    $destinationTrackingCategoryId = $matchedDestinationCategory['TrackingCategoryID'];

                    $this->storeMapping(
                        $mapper,
                        $destinationRole,
                        self::ENTITY,
                        $source,
                        $destination,
                        $sourceTrackingCategoryId,
                        $destinationTrackingCategoryId,
                        $trackingCategoryName
                    );

                    $verifiedId = $destinationRole === 'test'
                        ? $mapper->getTestId(self::ENTITY, $sourceTrackingCategoryId)
                        : $mapper->getTargetId(self::ENTITY, $sourceTrackingCategoryId);

                    if (!$verifiedId || $verifiedId !== $destinationTrackingCategoryId) {
                        $this->error("Category mapping verification failed for recovered category: {$trackingCategoryName}");
                        return Command::FAILURE;
                    }

                    $recovered++;
                    $this->info("Recovered + mapped category {$trackingCategoryName} → {$destinationTrackingCategoryId}");
                }
            }

            if (!$destinationTrackingCategory) {
                $categoryPayload = $this->sanitizeCategory($trackingCategory);

                if (empty($categoryPayload['Name'])) {
                    $this->error("Sanitized tracking category has no Name: {$trackingCategoryName}");
                    $this->line(json_encode($trackingCategory, JSON_PRETTY_PRINT | JSON_UNESCAPED_SLASHES));
                    return Command::FAILURE;
                }

                $putCategory = $this->putTrackingCategoryWithRetry($destination, $categoryPayload, $trackingCategoryName);
                $destinationTrackingCategoryId = $putCategory->json('TrackingCategories.0.TrackingCategoryID');

                if ($putCategory->successful() && $destinationTrackingCategoryId) {
                    $this->storeMapping(
                        $mapper,
                        $destinationRole,
                        self::ENTITY,
                        $source,
                        $destination,
                        $sourceTrackingCategoryId,
                        $destinationTrackingCategoryId,
                        $trackingCategoryName
                    );

                    $verifiedId = $destinationRole === 'test'
                        ? $mapper->getTestId(self::ENTITY, $sourceTrackingCategoryId)
                        : $mapper->getTargetId(self::ENTITY, $sourceTrackingCategoryId);

                    if (!$verifiedId || $verifiedId !== $destinationTrackingCategoryId) {
                        $this->error("Category mapping verification failed for created category: {$trackingCategoryName}");
                        return Command::FAILURE;
                    }

                    $destinationCategories = $this->fetchTrackingCategories($destination, strtoupper($destinationRole));
                    [$destinationById, $destinationByName] = $this->buildDestinationIndexes($destinationCategories);
                    $destinationTrackingCategory = $destinationById->get($destinationTrackingCategoryId);

                    $created++;
                    $this->info("Created + mapped category {$trackingCategoryName} → {$destinationTrackingCategoryId}");
                } else {
                    $this->warn("Create not completed -> refreshing destination index and retrying recovery for category: {$trackingCategoryName}");

                    $destinationCategories = $this->fetchTrackingCategories($destination, strtoupper($destinationRole));
                    [$destinationById, $destinationByName] = $this->buildDestinationIndexes($destinationCategories);

                    $matchedDestinationCategory = $this->findDestinationCategoryMatch($trackingCategory, $destinationByName);

                    if ($matchedDestinationCategory) {
                        $destinationTrackingCategory = $matchedDestinationCategory;
                        $destinationTrackingCategoryId = $matchedDestinationCategory['TrackingCategoryID'];

                        $this->storeMapping(
                            $mapper,
                            $destinationRole,
                            self::ENTITY,
                            $source,
                            $destination,
                            $sourceTrackingCategoryId,
                            $destinationTrackingCategoryId,
                            $trackingCategoryName
                        );

                        $verifiedId = $destinationRole === 'test'
                            ? $mapper->getTestId(self::ENTITY, $sourceTrackingCategoryId)
                            : $mapper->getTargetId(self::ENTITY, $sourceTrackingCategoryId);

                        if (!$verifiedId || $verifiedId !== $destinationTrackingCategoryId) {
                            $this->error("Category mapping verification failed after recovery for: {$trackingCategoryName}");
                            return Command::FAILURE;
                        }

                        $recovered++;
                        $this->info("Recovered after refresh + mapped category {$trackingCategoryName} → {$destinationTrackingCategoryId}");
                    } else {
                        $this->error("Unable to create or recover tracking category: {$trackingCategoryName}");
                        $this->line('HTTP Status: ' . $putCategory->status());
                        $this->line($putCategory->body());
                        $this->line(json_encode($categoryPayload, JSON_PRETTY_PRINT | JSON_UNESCAPED_SLASHES));

                        Log::error('Xero tracking category migration failed', [
                            'category_name' => $trackingCategoryName,
                            'payload' => $categoryPayload,
                            'status' => $putCategory->status(),
                            'response' => $putCategory->body(),
                        ]);

                        return Command::FAILURE;
                    }
                }
            }

            if (!$destinationTrackingCategoryId) {
                $this->error("No destination category ID resolved for {$trackingCategoryName}.");
                return Command::FAILURE;
            }

            $destinationTrackingCategory = $destinationTrackingCategory
                ?? $destinationById->get($destinationTrackingCategoryId)
                ?? [];

            $destinationOptionsById = collect($destinationTrackingCategory['Options'] ?? [])
                ->filter(fn ($option) => !empty($option['TrackingOptionID']))
                ->keyBy('TrackingOptionID');

            $destinationOptionsByName = collect($destinationTrackingCategory['Options'] ?? [])
                ->filter(fn ($option) => !empty($option['Name']))
                ->groupBy(fn ($option) => $this->normalizeName($option['Name']));

            $sourceOptions = collect($trackingCategory['Options'] ?? []);

            foreach ($sourceOptions as $sourceOption) {
                $sourceOptionId = $sourceOption['TrackingOptionID'] ?? null;
                $sourceOptionName = $sourceOption['Name'] ?? null;

                if (!$sourceOptionId || !$sourceOptionName) {
                    $this->error("Invalid source tracking option in category {$trackingCategoryName}.");
                    $this->line(json_encode($sourceOption, JSON_PRETTY_PRINT | JSON_UNESCAPED_SLASHES));
                    return Command::FAILURE;
                }

                $optionSourceKey = $this->buildOptionSourceKey($sourceTrackingCategoryId, $sourceOptionId);

                $mappedOptionId = $destinationRole === 'test'
                    ? $mapper->getTestId(self::OPTION_ENTITY, $optionSourceKey)
                    : $mapper->getTargetId(self::OPTION_ENTITY, $optionSourceKey);

                $destinationOption = null;
                $destinationOptionId = null;

                if ($mappedOptionId) {
                    $mappedOption = $destinationOptionsById->get($mappedOptionId);

                    if ($mappedOption && $this->isReasonableMappedOptionMatch($sourceOption, $mappedOption)) {
                        $destinationOption = $mappedOption;
                        $destinationOptionId = $mappedOptionId;
                        $optionsMappedValid++;
                        $this->info("Mapped already → Valid option: {$sourceOptionName} ({$destinationOptionId})");
                    }
                }

                if (!$destinationOption) {
                    $matchedDestinationOption = $this->findDestinationOptionMatch($sourceOption, $destinationOptionsByName);

                    if ($matchedDestinationOption) {
                        $destinationOption = $matchedDestinationOption;
                        $destinationOptionId = $matchedDestinationOption['TrackingOptionID'];

                        $this->storeMapping(
                            $mapper,
                            $destinationRole,
                            self::OPTION_ENTITY,
                            $source,
                            $destination,
                            $optionSourceKey,
                            $destinationOptionId,
                            $sourceOptionName
                        );

                        $verifiedOptionId = $destinationRole === 'test'
                            ? $mapper->getTestId(self::OPTION_ENTITY, $optionSourceKey)
                            : $mapper->getTargetId(self::OPTION_ENTITY, $optionSourceKey);

                        if (!$verifiedOptionId || $verifiedOptionId !== $destinationOptionId) {
                            $this->error("Option mapping verification failed for recovered option: {$sourceOptionName}");
                            return Command::FAILURE;
                        }

                        $optionsRecovered++;
                        $this->info("Recovered + mapped option {$sourceOptionName} → {$destinationOptionId}");
                    }
                }

                if (!$destinationOption) {
                    $optionPayload = $this->sanitizeOption($sourceOption);

                    if (empty($optionPayload['Name'])) {
                        $this->error("Sanitized tracking option has no Name: {$sourceOptionName}");
                        $this->line(json_encode($sourceOption, JSON_PRETTY_PRINT | JSON_UNESCAPED_SLASHES));
                        return Command::FAILURE;
                    }

                    $putOption = $this->putTrackingOptionWithRetry(
                        $destination,
                        $destinationTrackingCategoryId,
                        $optionPayload,
                        $trackingCategoryName,
                        $sourceOptionName
                    );

                    $destinationOptionId = $putOption->json('Options.0.TrackingOptionID')
                        ?? $putOption->json('TrackingOptions.0.TrackingOptionID')
                        ?? $putOption->json('TrackingCategory.Options.0.TrackingOptionID')
                        ?? null;

                    if ($putOption->successful() && $destinationOptionId) {
                        $this->storeMapping(
                            $mapper,
                            $destinationRole,
                            self::OPTION_ENTITY,
                            $source,
                            $destination,
                            $optionSourceKey,
                            $destinationOptionId,
                            $sourceOptionName
                        );

                        $verifiedOptionId = $destinationRole === 'test'
                            ? $mapper->getTestId(self::OPTION_ENTITY, $optionSourceKey)
                            : $mapper->getTargetId(self::OPTION_ENTITY, $optionSourceKey);

                        if (!$verifiedOptionId || $verifiedOptionId !== $destinationOptionId) {
                            $this->error("Option mapping verification failed for created option: {$sourceOptionName}");
                            return Command::FAILURE;
                        }

                        $destinationCategories = $this->fetchTrackingCategories($destination, strtoupper($destinationRole));
                        [$destinationById, $destinationByName] = $this->buildDestinationIndexes($destinationCategories);
                        $destinationTrackingCategory = $destinationById->get($destinationTrackingCategoryId) ?? [];
                        $destinationOptionsById = collect($destinationTrackingCategory['Options'] ?? [])
                            ->filter(fn ($option) => !empty($option['TrackingOptionID']))
                            ->keyBy('TrackingOptionID');
                        $destinationOptionsByName = collect($destinationTrackingCategory['Options'] ?? [])
                            ->filter(fn ($option) => !empty($option['Name']))
                            ->groupBy(fn ($option) => $this->normalizeName($option['Name']));

                        $optionsCreated++;
                        $this->info("Created + mapped option {$sourceOptionName} → {$destinationOptionId}");
                        continue;
                    }

                    $this->warn("Create not completed -> refreshing destination category index and retrying recovery for option: {$sourceOptionName}");

                    $destinationCategories = $this->fetchTrackingCategories($destination, strtoupper($destinationRole));
                    [$destinationById, $destinationByName] = $this->buildDestinationIndexes($destinationCategories);
                    $destinationTrackingCategory = $destinationById->get($destinationTrackingCategoryId) ?? [];
                    $destinationOptionsById = collect($destinationTrackingCategory['Options'] ?? [])
                        ->filter(fn ($option) => !empty($option['TrackingOptionID']))
                        ->keyBy('TrackingOptionID');
                    $destinationOptionsByName = collect($destinationTrackingCategory['Options'] ?? [])
                        ->filter(fn ($option) => !empty($option['Name']))
                        ->groupBy(fn ($option) => $this->normalizeName($option['Name']));

                    $matchedDestinationOption = $this->findDestinationOptionMatch($sourceOption, $destinationOptionsByName);

                    if ($matchedDestinationOption) {
                        $destinationOptionId = $matchedDestinationOption['TrackingOptionID'];

                        $this->storeMapping(
                            $mapper,
                            $destinationRole,
                            self::OPTION_ENTITY,
                            $source,
                            $destination,
                            $optionSourceKey,
                            $destinationOptionId,
                            $sourceOptionName
                        );

                        $verifiedOptionId = $destinationRole === 'test'
                            ? $mapper->getTestId(self::OPTION_ENTITY, $optionSourceKey)
                            : $mapper->getTargetId(self::OPTION_ENTITY, $optionSourceKey);

                        if (!$verifiedOptionId || $verifiedOptionId !== $destinationOptionId) {
                            $this->error("Option mapping verification failed after recovery for: {$sourceOptionName}");
                            return Command::FAILURE;
                        }

                        $optionsRecovered++;
                        $this->info("Recovered after refresh + mapped option {$sourceOptionName} → {$destinationOptionId}");
                    } else {
                        $this->error("Unable to create or recover tracking option: {$sourceOptionName}");
                        $this->line('HTTP Status: ' . $putOption->status());
                        $this->line($putOption->body());
                        $this->line(json_encode($optionPayload, JSON_PRETTY_PRINT | JSON_UNESCAPED_SLASHES));

                        Log::error('Xero tracking option migration failed', [
                            'category_name' => $trackingCategoryName,
                            'destination_category_id' => $destinationTrackingCategoryId,
                            'option_name' => $sourceOptionName,
                            'payload' => $optionPayload,
                            'status' => $putOption->status(),
                            'response' => $putOption->body(),
                        ]);

                        return Command::FAILURE;
                    }
                }
            }
        }

        $this->newLine();
        $this->info('Done.');
        $this->line("Categories processed: {$processed}");
        $this->line("Valid mapped categories: {$mappedValid}");
        $this->line("Invalid mapped categories found: {$mappedInvalid}");
        $this->line("Recovered + mapped categories: {$recovered}");
        $this->line("Created + mapped categories: {$created}");
        $this->line("Valid mapped options: {$optionsMappedValid}");
        $this->line("Recovered + mapped options: {$optionsRecovered}");
        $this->line("Created + mapped options: {$optionsCreated}");

        return Command::SUCCESS;
    }

    private function fetchTrackingCategories(XeroOrganisation $organisation, string $label): Collection
    {
        $response = $this->getTrackingCategoriesWithRetry($organisation, $label);

        return collect($response->json('TrackingCategories', []));
    }

    private function getTrackingCategoriesWithRetry(XeroOrganisation $organisation, string $label): Response
    {
        $maxAttempts = 6;
        $attempt = 1;

        while ($attempt <= $maxAttempts) {
            $response = Http::withToken($organisation->token->access_token)
                ->withHeaders([
                    'Xero-tenant-id' => $organisation->tenant_id,
                    'Accept' => 'application/json',
                ])
                ->get('https://api.xero.com/api.xro/2.0/TrackingCategories');

            if ($response->successful()) {
                return $response;
            }

            if ($response->status() === 429) {
                $retryAfter = (int) ($response->header('Retry-After') ?? 5);
                $retryAfter = $retryAfter > 0 ? $retryAfter : 5;

                $this->warn("Rate limited while fetching {$label} tracking categories. Waiting {$retryAfter} seconds before retry {$attempt}/{$maxAttempts}...");
                sleep($retryAfter);
                $attempt++;
                continue;
            }

            $this->error("Failed to fetch {$label} tracking categories.");
            $this->line('HTTP Status: ' . $response->status());
            $this->line($response->body());
            throw new \RuntimeException("Failed to fetch {$label} tracking categories.");
        }

        $this->error("Failed to fetch {$label} tracking categories after retries.");
        throw new \RuntimeException("Failed to fetch {$label} tracking categories after retries.");
    }

    private function putTrackingCategoryWithRetry(XeroOrganisation $organisation, array $payload, string $name): Response
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
                ->put('https://api.xero.com/api.xro/2.0/TrackingCategories', $payload);

            if ($response->successful()) {
                return $response;
            }

            if ($response->status() === 429) {
                $retryAfter = (int) ($response->header('Retry-After') ?? 5);
                $retryAfter = $retryAfter > 0 ? $retryAfter : 5;

                $this->warn("Rate limited while creating tracking category {$name}. Waiting {$retryAfter} seconds before retry {$attempt}/{$maxAttempts}...");
                sleep($retryAfter);
                $attempt++;
                continue;
            }

            return $response;
        }

        $this->error("Rate limited while creating tracking category {$name} after retries.");
        throw new \RuntimeException("Rate limited while creating tracking category {$name} after retries.");
    }

    private function putTrackingOptionWithRetry(
        XeroOrganisation $organisation,
        string $destinationTrackingCategoryId,
        array $payload,
        string $categoryName,
        string $optionName
    ): Response {
        $maxAttempts = 6;
        $attempt = 1;

        while ($attempt <= $maxAttempts) {
            $response = Http::withToken($organisation->token->access_token)
                ->withHeaders([
                    'Xero-tenant-id' => $organisation->tenant_id,
                    'Accept' => 'application/json',
                    'Content-Type' => 'application/json',
                ])
                ->put(
                    "https://api.xero.com/api.xro/2.0/TrackingCategories/{$destinationTrackingCategoryId}/Options",
                    $payload
                );

            if ($response->successful()) {
                return $response;
            }

            if ($response->status() === 429) {
                $retryAfter = (int) ($response->header('Retry-After') ?? 5);
                $retryAfter = $retryAfter > 0 ? $retryAfter : 5;

                $this->warn("Rate limited while creating option {$optionName} in {$categoryName}. Waiting {$retryAfter} seconds before retry {$attempt}/{$maxAttempts}...");
                sleep($retryAfter);
                $attempt++;
                continue;
            }

            return $response;
        }

        $this->error("Rate limited while creating option {$optionName} after retries.");
        throw new \RuntimeException("Rate limited while creating option {$optionName} after retries.");
    }

    private function buildDestinationIndexes(Collection $categories): array
    {
        $byId = $categories
            ->filter(fn ($category) => !empty($category['TrackingCategoryID']))
            ->keyBy('TrackingCategoryID');

        $byName = $categories
            ->filter(fn ($category) => !empty($category['Name']))
            ->groupBy(fn ($category) => $this->normalizeName($category['Name']));

        return [$byId, $byName];
    }

    private function filterSourceCategories(Collection $categories): Collection
    {
        if ($this->option('source-id')) {
            $sourceId = trim((string) $this->option('source-id'));

            return $categories
                ->filter(fn (array $category) => ($category['TrackingCategoryID'] ?? '') === $sourceId)
                ->values();
        }

        if ($this->option('name')) {
            $name = trim((string) $this->option('name'));

            return $categories
                ->filter(fn (array $category) => trim((string) ($category['Name'] ?? '')) === $name)
                ->values();
        }

        return $categories->values();
    }

    private function findDestinationCategoryMatch(array $sourceCategory, Collection $destinationByName): ?array
    {
        $nameKey = $this->normalizeName($sourceCategory['Name'] ?? '');

        if ($nameKey === '') {
            return null;
        }

        $matches = $destinationByName->get($nameKey);

        if ($matches && $matches->count() === 1) {
            return $matches->first();
        }

        return null;
    }

    private function findDestinationOptionMatch(array $sourceOption, Collection $destinationOptionsByName): ?array
    {
        $nameKey = $this->normalizeName($sourceOption['Name'] ?? '');

        if ($nameKey === '') {
            return null;
        }

        $matches = $destinationOptionsByName->get($nameKey);

        if ($matches && $matches->count() === 1) {
            return $matches->first();
        }

        return null;
    }

    private function isReasonableMappedCategoryMatch(array $sourceCategory, array $destinationCategory): bool
    {
        $sourceName = $this->normalizeName($sourceCategory['Name'] ?? '');
        $destinationName = $this->normalizeName($destinationCategory['Name'] ?? '');

        return $sourceName !== '' && $destinationName !== '' && $sourceName === $destinationName;
    }

    private function isReasonableMappedOptionMatch(array $sourceOption, array $destinationOption): bool
    {
        $sourceName = $this->normalizeName($sourceOption['Name'] ?? '');
        $destinationName = $this->normalizeName($destinationOption['Name'] ?? '');

        return $sourceName !== '' && $destinationName !== '' && $sourceName === $destinationName;
    }

    private function sanitizeCategory(array $trackingCategory): array
    {
        return collect($trackingCategory)
            ->except([
                'TrackingCategoryID',
                'HasValidationErrors',
                'Status',
                'Options',
            ])
            ->filter(fn ($value) => $value !== null && $value !== '')
            ->toArray();
    }

    private function sanitizeOption(array $trackingOption): array
    {
        return collect($trackingOption)
            ->except([
                'TrackingOptionID',
                'HasValidationErrors',
                'Status',
            ])
            ->filter(fn ($value) => $value !== null && $value !== '')
            ->toArray();
    }

    private function normalizeName(?string $value): string
    {
        if (!$value) {
            return '';
        }

        return mb_strtolower(trim($value));
    }

    private function buildOptionSourceKey(string $trackingCategoryId, string $trackingOptionId): string
    {
        return $trackingCategoryId . ':' . $trackingOptionId;
    }

    private function storeMapping(
        XeroIdMapper $mapper,
        string $destinationRole,
        string $entity,
        XeroOrganisation $source,
        XeroOrganisation $destination,
        string $sourceId,
        string $destinationId,
        string $name
    ): void {
        if ($destinationRole === 'test') {
            $mapper->storeTest(
                $entity,
                $sourceId,
                $destinationId,
                $source->tenant_id,
                $destination->tenant_id,
                $name
            );
            return;
        }

        $mapper->storeTarget(
            $entity,
            $sourceId,
            $destinationId,
            $source->tenant_id,
            $destination->tenant_id,
            $name
        );
    }
}