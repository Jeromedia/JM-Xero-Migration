<?php

namespace App\Console\Commands\TaxRates;

use App\Models\XeroOrganisation;
use Illuminate\Console\Command;
use Illuminate\Support\Facades\Http;
use Illuminate\Support\Facades\Log;

class SyncTaxRates extends Command
{
    protected $signature = 'xero:taxrates:sync {--live : Use target instead of test}';
    protected $description = 'Sync tax rates from SOURCE to TEST or TARGET';

    public function handle(): int
    {
        $destinationRole = $this->option('live') ? 'target' : 'test';

        $this->info('Starting tax rates synchronization...');
        $this->info('Source role: source');
        $this->info('Destination role: ' . $destinationRole);

        $mode = $this->choice(
            'Select migration mode',
            [
                'Test (1 tax rate only)',
                'Batch (all tax rates)'
            ],
            0
        );

        $testMode = $mode === 'Test (1 tax rate only)';

        $source = XeroOrganisation::where('role', 'source')->first();
        $destination = XeroOrganisation::where('role', $destinationRole)->first();

        /*
        |--------------------------------------------------------------------------
        | Fetch SOURCE tax rates
        |--------------------------------------------------------------------------
        */

        $this->info("Fetching SOURCE tax rates...");

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

        if ($testMode) {
            $sourceRates = $sourceRates->take(1);
        }

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

        $destinationTaxTypes = $destinationRates
            ->pluck('TaxType')
            ->toArray();

        /*
        |--------------------------------------------------------------------------
        | Process tax rates
        |--------------------------------------------------------------------------
        */

        foreach ($sourceRates as $rate) {

            $taxType = $rate['TaxType'];

            if (in_array($taxType, $destinationTaxTypes)) {

                $this->info("Already exists. Skipping: {$taxType}");
                continue;
            }

            /*
            |--------------------------------------------------------------------------
            | Clean payload (remove read-only fields)
            |--------------------------------------------------------------------------
            */

            $cleanRate = collect($rate)
                ->except([
                    'TaxType',
                    'CanApplyToAssets',
                    'CanApplyToEquity',
                    'CanApplyToExpenses',
                    'CanApplyToLiabilities',
                    'CanApplyToRevenue',
                    'DisplayTaxRate',
                    'EffectiveRate',
                    'HasValidationErrors',
                    'Status'
                ])
                ->filter()
                ->toArray();

            $payload = [
                'TaxRates' => [
                    $cleanRate
                ]
            ];

            $jsonPayload = json_encode(
                $payload,
                JSON_PRETTY_PRINT | JSON_UNESCAPED_SLASHES
            );

            $this->warn("Next tax rate: {$rate['Name']}");
            $this->line($jsonPayload);

            if (!$this->confirm('Send this tax rate now?', false)) {
                return Command::SUCCESS;
            }

            /*
            |--------------------------------------------------------------------------
            | Send tax rate
            |--------------------------------------------------------------------------
            */

            $post = Http::withToken($destination->token->access_token)
                ->withHeaders([
                    'Xero-tenant-id' => $destination->tenant_id,
                    'Accept' => 'application/json',
                    'Content-Type' => 'application/json'
                ])
                ->post(
                    'https://api.xero.com/api.xro/2.0/TaxRates',
                    $payload
                );

            if (!$post->successful()) {

                $this->error("Failed to add {$rate['Name']}");

                Log::error('Xero tax rate migration failed', [
                    'payload' => $payload,
                    'response' => $post->body()
                ]);

                continue;
            }

            $this->info("Added {$rate['Name']}");

            if ($testMode) {
                return Command::SUCCESS;
            }

            usleep(500000);
        }

        $this->info('Finished.');
        return Command::SUCCESS;
    }
}