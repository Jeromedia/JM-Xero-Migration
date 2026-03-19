<?php

namespace App\Console\Commands\Contacts;

use App\Models\XeroOrganisation;
use Illuminate\Console\Command;
use Illuminate\Support\Facades\Http;

class CompareContactGroups extends Command
{
    protected $signature = 'xero:contact-groups:compare {--live : Use target instead of test}';
    protected $description = 'Compare contact groups between SOURCE and TEST or TARGET';

    public function handle(): int
    {
        $destinationRole = $this->option('live') ? 'target' : 'test';

        $this->info('Starting contact groups comparison...');
        $this->info('Source role: source');
        $this->info('Destination role: ' . $destinationRole);

        $source = XeroOrganisation::where('role', 'source')->first();
        $destination = XeroOrganisation::where('role', $destinationRole)->first();

        if (!$source || !$destination) {
            $this->error('Source or destination organisation not found.');
            return Command::FAILURE;
        }

        // 🔹 FETCH SOURCE
        $sourceResponse = Http::withToken($source->token->access_token)
            ->withHeaders([
                'Xero-tenant-id' => $source->tenant_id,
                'Accept' => 'application/json',
            ])
            ->get('https://api.xero.com/api.xro/2.0/ContactGroups');

        if (!$sourceResponse->successful()) {
            $this->error('Failed to fetch SOURCE contact groups.');
            return Command::FAILURE;
        }

        // 🔹 FETCH DESTINATION
        $destinationResponse = Http::withToken($destination->token->access_token)
            ->withHeaders([
                'Xero-tenant-id' => $destination->tenant_id,
                'Accept' => 'application/json',
            ])
            ->get('https://api.xero.com/api.xro/2.0/ContactGroups');

        if (!$destinationResponse->successful()) {
            $this->error('Failed to fetch destination contact groups.');
            return Command::FAILURE;
        }

        $sourceGroups = collect($sourceResponse->json('ContactGroups', []));
        $destinationGroups = collect($destinationResponse->json('ContactGroups', []));

        $sourceNames = $sourceGroups->pluck('Name')->filter()->sort()->values();
        $destinationNames = $destinationGroups->pluck('Name')->filter()->sort()->values();

        $missing = $sourceNames->diff($destinationNames)->values();
        $extra = $destinationNames->diff($sourceNames)->values();
        $matching = $sourceNames->intersect($destinationNames)->values();

        // 🔹 SUMMARY
        $this->newLine();
        $this->info('Summary');
        $this->line('SOURCE groups: ' . $sourceGroups->count());
        $this->line(strtoupper($destinationRole) . ' groups: ' . $destinationGroups->count());

        $this->newLine();
        $this->info('Comparison');
        $this->line('Matching: ' . $matching->count());
        $this->line('Missing in ' . strtoupper($destinationRole) . ': ' . $missing->count());
        $this->line('Extra in ' . strtoupper($destinationRole) . ': ' . $extra->count());

        // 🔹 DETAILS
        if ($missing->isNotEmpty()) {
            $this->newLine();
            $this->warn('Missing groups:');
            foreach ($missing as $name) {
                $this->line(' - ' . $name);
            }
        }

        if ($extra->isNotEmpty()) {
            $this->newLine();
            $this->warn('Extra groups:');
            foreach ($extra as $name) {
                $this->line(' - ' . $name);
            }
        }

        $this->newLine();
        $this->info('Comparison complete.');

        return Command::SUCCESS;
    }
}