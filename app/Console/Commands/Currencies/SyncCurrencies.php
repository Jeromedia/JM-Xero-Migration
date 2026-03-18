<?php

namespace App\Console\Commands\Currencies;

use App\Models\XeroOrganisation;
use Illuminate\Console\Command;
use Illuminate\Support\Facades\Http;
use Illuminate\Support\Facades\Log;

class SyncCurrencies extends Command
{
    protected $signature = 'xero:currencies:sync {--live : Use target instead of test}';
    protected $description = 'Sync currencies from SOURCE to TEST by default, or to TARGET with --live';

    public function handle(): int
    {
        $destinationRole = $this->option('live') ? 'target' : 'test';

        $this->info('Starting currencies synchronization...');
        $this->info('Source role: source');
        $this->info('Destination role: ' . $destinationRole);

        $source = XeroOrganisation::where('role', 'source')->first();
        $destination = XeroOrganisation::where('role', $destinationRole)->first();

        if (!$source) {
            $this->error('Source organisation not found.');
            return self::FAILURE;
        }

        if (!$destination) {
            $this->error("Destination organisation not found for role: {$destinationRole}");
            return self::FAILURE;
        }

        if ($source->tenant_id === $destination->tenant_id) {
            $this->error('Source and destination tenants are identical. Aborting.');
            return self::FAILURE;
        }

        $sourceToken = $source->token;
        $destinationToken = $destination->token;

        if (!$sourceToken || !$destinationToken) {
            $this->error('Missing token relationship on source or destination organisation.');
            return self::FAILURE;
        }

        if (empty($sourceToken->access_token) || empty($destinationToken->access_token)) {
            $this->error('Missing access token on source or destination token row.');
            return self::FAILURE;
        }

        $this->info('Fetching SOURCE currencies...');
        $sourceResponse = Http::withToken($sourceToken->access_token)
            ->withHeaders([
                'Xero-tenant-id' => $source->tenant_id,
                'Accept' => 'application/json',
            ])
            ->get('https://api.xero.com/api.xro/2.0/Currencies');

        if (!$sourceResponse->successful()) {
            $this->error('Failed to fetch SOURCE currencies.');
            $this->line('HTTP Status: ' . $sourceResponse->status());
            $this->line($sourceResponse->body());

            Log::error('Xero GET /Currencies failed (SOURCE)', [
                'tenant_id' => $source->tenant_id,
                'status' => $sourceResponse->status(),
                'body' => $sourceResponse->body(),
            ]);

            return self::FAILURE;
        }

        $sourceCurrencies = collect($sourceResponse->json('Currencies') ?? [])
            ->map(function ($currency) {
                return [
                    'Code' => strtoupper(trim((string) ($currency['Code'] ?? ''))),
                    'Description' => trim((string) ($currency['Description'] ?? '')),
                ];
            })
            ->filter(fn ($currency) => $currency['Code'] !== '')
            ->unique('Code')
            ->values();

        $this->info('Fetching ' . strtoupper($destinationRole) . ' currencies...');
        $destinationResponse = Http::withToken($destinationToken->access_token)
            ->withHeaders([
                'Xero-tenant-id' => $destination->tenant_id,
                'Accept' => 'application/json',
            ])
            ->get('https://api.xero.com/api.xro/2.0/Currencies');

        if (!$destinationResponse->successful()) {
            $this->error('Failed to fetch ' . strtoupper($destinationRole) . ' currencies.');
            $this->line('HTTP Status: ' . $destinationResponse->status());
            $this->line($destinationResponse->body());

            Log::error('Xero GET /Currencies failed (DESTINATION)', [
                'destination_role' => $destinationRole,
                'tenant_id' => $destination->tenant_id,
                'status' => $destinationResponse->status(),
                'body' => $destinationResponse->body(),
            ]);

            return self::FAILURE;
        }

        $destinationCodes = collect($destinationResponse->json('Currencies') ?? [])
            ->pluck('Code')
            ->map(fn ($code) => strtoupper(trim((string) $code)))
            ->filter(fn ($code) => $code !== '')
            ->unique()
            ->values();

        $missingCurrencies = $sourceCurrencies
            ->filter(fn ($currency) => !$destinationCodes->contains($currency['Code']))
            ->values();

        $this->line('');
        $this->info('SOURCE tenant: ' . ($source->tenant_name ?? $source->tenant_id));
        $this->info(strtoupper($destinationRole) . ' tenant: ' . ($destination->tenant_name ?? $destination->tenant_id));
        $this->line('');
        $this->info('SOURCE currencies: ' . ($sourceCurrencies->isEmpty() ? '(none)' : $sourceCurrencies->pluck('Code')->join(', ')));
        $this->info(strtoupper($destinationRole) . ' currencies: ' . ($destinationCodes->isEmpty() ? '(none)' : $destinationCodes->join(', ')));
        $this->line('');

        if ($missingCurrencies->isEmpty()) {
            $this->info('Nothing to add. Destination already contains all source currencies.');
            return self::SUCCESS;
        }

        $this->warn('Currencies that will be added:');
        foreach ($missingCurrencies as $currency) {
            $label = $currency['Code'];
            if ($currency['Description'] !== '') {
                $label .= ' - ' . $currency['Description'];
            }
            $this->line(' - ' . $label);
        }

        $this->line('');

        if (!$this->confirm('Do you want to continue with these currencies for the ' . strtoupper($destinationRole) . ' organisation?', false)) {
            $this->warn('Operation cancelled.');
            return self::SUCCESS;
        }

        $added = [];
        $failed = [];

        foreach ($missingCurrencies as $currency) {
            $payload = [
                'Currencies' => [
                    [
                        'Code' => $currency['Code'],
                        'Description' => $currency['Description'],
                    ],
                ],
            ];

            $jsonPayload = json_encode($payload, JSON_PRETTY_PRINT | JSON_UNESCAPED_SLASHES | JSON_UNESCAPED_UNICODE);

            $this->line('');
            $this->warn('Next currency: ' . $currency['Code']);
            $this->line('Payload to be sent:');
            $this->line($jsonPayload);
            $this->line('');

            if (!$this->confirm('Send this currency now?', false)) {
                $this->warn('Command aborted by user.');
                $this->line('');

                if (!empty($added)) {
                    $this->info('Added before abort: ' . implode(', ', $added));
                }

                if (!empty($failed)) {
                    $this->warn('Failed before abort: ' . implode(', ', $failed));
                }

                return self::SUCCESS;
            }

            $this->info('Sending ' . $currency['Code'] . ' to ' . strtoupper($destinationRole) . '...');

            $response = Http::withToken($destinationToken->access_token)
                ->withHeaders([
                    'Xero-tenant-id' => $destination->tenant_id,
                    'Accept' => 'application/json',
                    'Content-Type' => 'application/json',
                ])
                ->withBody($jsonPayload, 'application/json')
                ->send('PUT', 'https://api.xero.com/api.xro/2.0/Currencies');

            if ($response->successful()) {
                $this->info('Added ' . $currency['Code']);
                $added[] = $currency['Code'];
                continue;
            }

            $this->error('Failed to add ' . $currency['Code']);
            $this->line('HTTP Status: ' . $response->status());
            $this->line($response->body());

            Log::error('Xero PUT /Currencies failed', [
                'tenant_id' => $destination->tenant_id,
                'destination_role' => $destinationRole,
                'currency' => $currency,
                'status' => $response->status(),
                'body' => $response->body(),
                'payload' => $payload,
            ]);

            $failed[] = $currency['Code'];
        }

        $this->line('');
        $this->info('Finished.');

        if (!empty($added)) {
            $this->info('Added: ' . implode(', ', $added));
        }

        if (!empty($failed)) {
            $this->warn('Failed: ' . implode(', ', $failed));
            return self::FAILURE;
        }

        return self::SUCCESS;
    }
}