<?php

namespace App\Console\Commands\TaxRates;

use App\Models\XeroOrganisation;
use Illuminate\Console\Command;
use Illuminate\Support\Facades\Http;

class CompareTaxRates extends Command
{
    protected $signature = 'xero:taxrates:compare {--live : Compare against target instead of test}';
    protected $description = 'Compare tax rates between SOURCE and TEST or TARGET';

    public function handle(): int
    {
        $destinationRole = $this->option('live') ? 'target' : 'test';

        $this->info('Starting tax rates comparison...');
        $this->info('Source role: source');
        $this->info('Destination role: ' . $destinationRole);

        $source = XeroOrganisation::where('role', 'source')->first();
        $destination = XeroOrganisation::where('role', $destinationRole)->first();

        /*
        |--------------------------------------------------------------------------
        | Fetch SOURCE tax rates
        |--------------------------------------------------------------------------
        */

        $this->info('Fetching SOURCE tax rates...');

        $sourceResponse = Http::withToken($source->token->access_token)
            ->withHeaders([
                'Xero-tenant-id' => $source->tenant_id,
                'Accept' => 'application/json',
            ])
            ->get('https://api.xero.com/api.xro/2.0/TaxRates');

        if (!$sourceResponse->successful()) {
            $this->error('Failed to fetch SOURCE tax rates.');
            return Command::FAILURE;
        }

        $sourceRates = collect($sourceResponse->json('TaxRates'));

        /*
        |--------------------------------------------------------------------------
        | Fetch DESTINATION tax rates
        |--------------------------------------------------------------------------
        */

        $this->info("Fetching {$destinationRole} tax rates...");

        $destResponse = Http::withToken($destination->token->access_token)
            ->withHeaders([
                'Xero-tenant-id' => $destination->tenant_id,
                'Accept' => 'application/json',
            ])
            ->get('https://api.xero.com/api.xro/2.0/TaxRates');

        if (!$destResponse->successful()) {
            $this->error("Failed to fetch {$destinationRole} tax rates.");
            return Command::FAILURE;
        }

        $destinationRates = collect($destResponse->json('TaxRates'));

        /*
        |--------------------------------------------------------------------------
        | Summary counts
        |--------------------------------------------------------------------------
        */

        $this->line('');
        $this->info('Summary');
        $this->line('SOURCE tax rates: ' . $sourceRates->count());
        $this->line(strtoupper($destinationRole) . ' tax rates: ' . $destinationRates->count());

        /*
        |--------------------------------------------------------------------------
        | Build lookup tables
        |--------------------------------------------------------------------------
        */

        $sourceMap = $sourceRates->keyBy('TaxType');
        $destMap = $destinationRates->keyBy('TaxType');

        $sourceTypes = $sourceMap->keys();
        $destTypes = $destMap->keys();

        /*
        |--------------------------------------------------------------------------
        | Comparison
        |--------------------------------------------------------------------------
        */

        $matching = $sourceTypes->intersect($destTypes);
        $missing = $sourceTypes->diff($destTypes);
        $extra = $destTypes->diff($sourceTypes);

        $this->line('');
        $this->info('Comparison');

        $this->line('Matching tax rates: ' . $matching->count());
        $this->line('Missing in ' . $destinationRole . ': ' . $missing->count());
        $this->line('Extra in ' . $destinationRole . ': ' . $extra->count());

        /*
        |--------------------------------------------------------------------------
        | Missing
        |--------------------------------------------------------------------------
        */

        if ($missing->isNotEmpty()) {

            $this->line('');
            $this->warn('Tax rates missing in ' . $destinationRole . ':');

            foreach ($missing as $type) {

                $name = $sourceMap[$type]['Name'];

                $this->line(" - {$type} ({$name})");
            }
        }

        /*
        |--------------------------------------------------------------------------
        | Extra
        |--------------------------------------------------------------------------
        */

        if ($extra->isNotEmpty()) {

            $this->line('');
            $this->warn('Extra tax rates in ' . $destinationRole . ':');

            foreach ($extra as $type) {

                $name = $destMap[$type]['Name'];

                $this->line(" - {$type} ({$name})");
            }
        }

        /*
        |--------------------------------------------------------------------------
        | Check rate differences
        |--------------------------------------------------------------------------
        */

        $differences = [];

        foreach ($matching as $type) {

            $sourceRate = $sourceMap[$type]['DisplayTaxRate'] ?? null;
            $destRate = $destMap[$type]['DisplayTaxRate'] ?? null;

            if ($sourceRate != $destRate) {

                $differences[] = [
                    'TaxType' => $type,
                    'SourceRate' => $sourceRate,
                    'DestinationRate' => $destRate
                ];
            }
        }

        if (!empty($differences)) {

            $this->line('');
            $this->warn('Tax rate differences detected:');

            $this->table(
                ['TaxType', 'Source Rate', 'Destination Rate'],
                $differences
            );
        }

        $this->line('');
        $this->info('Comparison complete.');

        return Command::SUCCESS;
    }
}