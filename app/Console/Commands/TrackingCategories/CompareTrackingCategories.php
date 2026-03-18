<?php

namespace App\Console\Commands\TrackingCategories;

use App\Models\XeroOrganisation;
use Illuminate\Console\Command;
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

        $this->info('Fetching SOURCE tracking categories...');
        $sourceResponse = Http::withToken($source->token->access_token)
            ->withHeaders([
                'Xero-tenant-id' => $source->tenant_id,
                'Accept' => 'application/json',
            ])
            ->get('https://api.xero.com/api.xro/2.0/TrackingCategories');

        if (!$sourceResponse->successful()) {
            $this->error('Failed to fetch SOURCE tracking categories.');
            return Command::FAILURE;
        }

        $this->info('Fetching ' . strtoupper($destinationRole) . ' tracking categories...');
        $destinationResponse = Http::withToken($destination->token->access_token)
            ->withHeaders([
                'Xero-tenant-id' => $destination->tenant_id,
                'Accept' => 'application/json',
            ])
            ->get('https://api.xero.com/api.xro/2.0/TrackingCategories');

        if (!$destinationResponse->successful()) {
            $this->error('Failed to fetch destination tracking categories.');
            return Command::FAILURE;
        }

        $sourceCategories = collect($sourceResponse->json('TrackingCategories', []));
        $destinationCategories = collect($destinationResponse->json('TrackingCategories', []));

        $sourceCategoryNames = $sourceCategories->pluck('Name')->sort()->values();
        $destinationCategoryNames = $destinationCategories->pluck('Name')->sort()->values();

        $missingCategories = $sourceCategoryNames->diff($destinationCategoryNames)->values();
        $extraCategories = $destinationCategoryNames->diff($sourceCategoryNames)->values();
        $matchingCategories = $sourceCategoryNames->intersect($destinationCategoryNames)->values();

        $sourceOptionsCount = $sourceCategories->sum(function (array $category) {
            return count($category['Options'] ?? []);
        });

        $destinationOptionsCount = $destinationCategories->sum(function (array $category) {
            return count($category['Options'] ?? []);
        });

        $missingOptionsByCategory = [];
        $extraOptionsByCategory = [];
        $matchingOptionsCount = 0;

        foreach ($matchingCategories as $categoryName) {
            $sourceCategory = $sourceCategories->firstWhere('Name', $categoryName);
            $destinationCategory = $destinationCategories->firstWhere('Name', $categoryName);

            $sourceOptions = collect($sourceCategory['Options'] ?? [])
                ->pluck('Name')
                ->filter()
                ->sort()
                ->values();

            $destinationOptions = collect($destinationCategory['Options'] ?? [])
                ->pluck('Name')
                ->filter()
                ->sort()
                ->values();

            $missingOptions = $sourceOptions->diff($destinationOptions)->values();
            $extraOptions = $destinationOptions->diff($sourceOptions)->values();
            $matchingOptions = $sourceOptions->intersect($destinationOptions)->values();

            $matchingOptionsCount += $matchingOptions->count();

            if ($missingOptions->isNotEmpty()) {
                $missingOptionsByCategory[$categoryName] = $missingOptions->all();
            }

            if ($extraOptions->isNotEmpty()) {
                $extraOptionsByCategory[$categoryName] = $extraOptions->all();
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

        $this->newLine();
        $this->info('Option Comparison');
        $this->line('Matching tracking options: ' . $matchingOptionsCount);
        $this->line('Missing options in ' . strtoupper($destinationRole) . ': ' . $missingOptionsTotal);
        $this->line('Extra options in ' . strtoupper($destinationRole) . ': ' . $extraOptionsTotal);

        if ($missingCategories->isNotEmpty()) {
            $this->newLine();
            $this->warn('Missing tracking categories in ' . strtoupper($destinationRole) . ':');
            foreach ($missingCategories as $name) {
                $this->line(' - ' . $name);
            }
        }

        if ($extraCategories->isNotEmpty()) {
            $this->newLine();
            $this->warn('Extra tracking categories in ' . strtoupper($destinationRole) . ':');
            foreach ($extraCategories as $name) {
                $this->line(' - ' . $name);
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

        $this->newLine();
        $this->info('Comparison complete.');

        return Command::SUCCESS;
    }
}