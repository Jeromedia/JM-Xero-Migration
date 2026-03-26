<?php

namespace App\Console\Commands\TrackingCategories;

use App\Models\XeroOrganisation;
use Illuminate\Console\Command;
use Illuminate\Http\Client\Response;
use Illuminate\Support\Collection;
use Illuminate\Support\Facades\Http;

class CompareTrackingCategories extends Command
{
    protected $signature = 'xero:tracking:compare {--live : Use target instead of test}';
    protected $description = 'Compare tracking categories and tracking options between SOURCE and TEST or TARGET';

    public function handle(): int
    {
        $destinationRole = $this->option('live') ? 'target' : 'test';

        $this->info('Starting tracking categories comparison...');
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

        $this->info('Fetching SOURCE tracking categories...');
        $sourceCategories = $this->fetchTrackingCategories($source, 'SOURCE');

        $this->info('Fetching ' . strtoupper($destinationRole) . ' tracking categories...');
        $destinationCategories = $this->fetchTrackingCategories($destination, strtoupper($destinationRole));

        $sourceByName = $this->indexCategoriesByName($sourceCategories);
        $destinationByName = $this->indexCategoriesByName($destinationCategories);

        $sourceCategoryNames = $sourceByName->keys()->sort()->values();
        $destinationCategoryNames = $destinationByName->keys()->sort()->values();

        $matchingCategories = $sourceCategoryNames->intersect($destinationCategoryNames)->values();
        $missingCategories = $sourceCategoryNames->diff($destinationCategoryNames)->values();
        $extraCategories = $destinationCategoryNames->diff($sourceCategoryNames)->values();

        $sourceOptionsCount = $sourceCategories->sum(fn (array $category) => collect($category['Options'] ?? [])->count());
        $destinationOptionsCount = $destinationCategories->sum(fn (array $category) => collect($category['Options'] ?? [])->count());

        $missingOptionsByCategory = [];
        $extraOptionsByCategory = [];
        $categoryStatusDifferences = [];
        $optionStatusDifferences = [];
        $matchingOptionsCount = 0;

        foreach ($matchingCategories as $categoryNameKey) {
            $sourceCategory = $sourceByName->get($categoryNameKey);
            $destinationCategory = $destinationByName->get($categoryNameKey);

            $sourceCategoryName = $sourceCategory['Name'] ?? $categoryNameKey;
            $sourceCategoryStatus = $sourceCategory['Status'] ?? '';
            $destinationCategoryStatus = $destinationCategory['Status'] ?? '';

            if ($sourceCategoryStatus !== $destinationCategoryStatus) {
                $categoryStatusDifferences[] = [
                    'Name' => $sourceCategoryName,
                    'SourceStatus' => $sourceCategoryStatus,
                    'DestinationStatus' => $destinationCategoryStatus,
                ];
            }

            $sourceOptions = collect($sourceCategory['Options'] ?? [])
                ->filter(fn ($option) => !empty($option['Name']))
                ->keyBy(fn ($option) => $this->normalizeName($option['Name']));

            $destinationOptions = collect($destinationCategory['Options'] ?? [])
                ->filter(fn ($option) => !empty($option['Name']))
                ->keyBy(fn ($option) => $this->normalizeName($option['Name']));

            $sourceOptionNames = $sourceOptions->keys()->sort()->values();
            $destinationOptionNames = $destinationOptions->keys()->sort()->values();

            $matchingOptionNames = $sourceOptionNames->intersect($destinationOptionNames)->values();
            $missingOptionNames = $sourceOptionNames->diff($destinationOptionNames)->values();
            $extraOptionNames = $destinationOptionNames->diff($sourceOptionNames)->values();

            $matchingOptionsCount += $matchingOptionNames->count();

            if ($missingOptionNames->isNotEmpty()) {
                $missingOptionsByCategory[$sourceCategoryName] = $missingOptionNames
                    ->map(fn ($key) => $sourceOptions->get($key)['Name'] ?? $key)
                    ->values()
                    ->all();
            }

            if ($extraOptionNames->isNotEmpty()) {
                $extraOptionsByCategory[$sourceCategoryName] = $extraOptionNames
                    ->map(fn ($key) => $destinationOptions->get($key)['Name'] ?? $key)
                    ->values()
                    ->all();
            }

            foreach ($matchingOptionNames as $optionNameKey) {
                $sourceOption = $sourceOptions->get($optionNameKey);
                $destinationOption = $destinationOptions->get($optionNameKey);

                $sourceOptionStatus = $sourceOption['Status'] ?? '';
                $destinationOptionStatus = $destinationOption['Status'] ?? '';

                if ($sourceOptionStatus !== $destinationOptionStatus) {
                    $optionStatusDifferences[] = [
                        'Category' => $sourceCategoryName,
                        'Option' => $sourceOption['Name'] ?? $optionNameKey,
                        'SourceStatus' => $sourceOptionStatus,
                        'DestinationStatus' => $destinationOptionStatus,
                    ];
                }
            }
        }

        $missingOptionsTotal = collect($missingOptionsByCategory)->sum(fn ($options) => count($options));
        $extraOptionsTotal = collect($extraOptionsByCategory)->sum(fn ($options) => count($options));

        $this->newLine();
        $this->info('Summary');
        $this->line('SOURCE tracking categories: ' . $sourceCategories->count());
        $this->line(strtoupper($destinationRole) . ' tracking categories: ' . $destinationCategories->count());
        $this->line('SOURCE tracking options: ' . $sourceOptionsCount);
        $this->line(strtoupper($destinationRole) . ' tracking options: ' . $destinationOptionsCount);

        $this->newLine();
        $this->info('Category Comparison');
        $this->line('Matching tracking categories: ' . $matchingCategories->count());
        $this->line('Missing categories in ' . strtoupper($destinationRole) . ': ' . $missingCategories->count());
        $this->line('Extra categories in ' . strtoupper($destinationRole) . ': ' . $extraCategories->count());
        $this->line('Category status differences: ' . count($categoryStatusDifferences));

        $this->newLine();
        $this->info('Option Comparison');
        $this->line('Matching tracking options: ' . $matchingOptionsCount);
        $this->line('Missing options in ' . strtoupper($destinationRole) . ': ' . $missingOptionsTotal);
        $this->line('Extra options in ' . strtoupper($destinationRole) . ': ' . $extraOptionsTotal);
        $this->line('Option status differences: ' . count($optionStatusDifferences));

        if ($missingCategories->isNotEmpty()) {
            $this->newLine();
            $this->warn('Missing tracking categories in ' . strtoupper($destinationRole) . ':');
            foreach ($missingCategories as $nameKey) {
                $this->line(' - ' . ($sourceByName->get($nameKey)['Name'] ?? $nameKey));
            }
        }

        if ($extraCategories->isNotEmpty()) {
            $this->newLine();
            $this->warn('Extra tracking categories in ' . strtoupper($destinationRole) . ':');
            foreach ($extraCategories as $nameKey) {
                $this->line(' - ' . ($destinationByName->get($nameKey)['Name'] ?? $nameKey));
            }
        }

        if (!empty($categoryStatusDifferences)) {
            $this->newLine();
            $this->warn('Category status differences:');
            foreach ($categoryStatusDifferences as $row) {
                $this->line(sprintf(
                    ' - %s | SOURCE: %s | DESTINATION: %s',
                    $row['Name'],
                    $row['SourceStatus'],
                    $row['DestinationStatus']
                ));
            }
        }

        if (!empty($missingOptionsByCategory)) {
            $this->newLine();
            $this->warn('Missing tracking options in ' . strtoupper($destinationRole) . ':');
            foreach ($missingOptionsByCategory as $categoryName => $options) {
                $this->line(" - {$categoryName}:");
                foreach ($options as $optionName) {
                    $this->line("    - {$optionName}");
                }
            }
        }

        if (!empty($extraOptionsByCategory)) {
            $this->newLine();
            $this->warn('Extra tracking options in ' . strtoupper($destinationRole) . ':');
            foreach ($extraOptionsByCategory as $categoryName => $options) {
                $this->line(" - {$categoryName}:");
                foreach ($options as $optionName) {
                    $this->line("    - {$optionName}");
                }
            }
        }

        if (!empty($optionStatusDifferences)) {
            $this->newLine();
            $this->warn('Option status differences:');
            foreach ($optionStatusDifferences as $row) {
                $this->line(sprintf(
                    ' - %s / %s | SOURCE: %s | DESTINATION: %s',
                    $row['Category'],
                    $row['Option'],
                    $row['SourceStatus'],
                    $row['DestinationStatus']
                ));
            }
        }

        $this->newLine();
        $this->info('Comparison complete.');

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

    private function indexCategoriesByName(Collection $categories): Collection
    {
        return $categories
            ->filter(fn ($category) => !empty($category['Name']))
            ->keyBy(fn ($category) => $this->normalizeName($category['Name']));
    }

    private function normalizeName(?string $value): string
    {
        if (!$value) {
            return '';
        }

        return mb_strtolower(trim($value));
    }
}