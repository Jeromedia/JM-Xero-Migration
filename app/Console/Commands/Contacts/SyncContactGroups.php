<?php

namespace App\Console\Commands\Contacts;

use App\Models\XeroOrganisation;
use App\Services\Xero\XeroIdMapper;
use Illuminate\Console\Command;
use Illuminate\Support\Facades\Http;
use Illuminate\Support\Facades\Log;

class SyncContactGroups extends Command
{
    protected $signature = 'xero:contact-groups:sync {--live : Use target instead of test}';
    protected $description = 'Sync contact groups from SOURCE to TEST or TARGET';

    private const ENTITY = 'contact_group';

    public function handle(): int
    {
        $destinationRole = $this->option('live') ? 'target' : 'test';

        $this->info('Starting contact groups synchronization...');
        $this->info('Source role: source');
        $this->info('Destination role: ' . $destinationRole);

        $mapper = new XeroIdMapper();

        $source = XeroOrganisation::where('role', 'source')->first();
        $destination = XeroOrganisation::where('role', $destinationRole)->first();

        if (!$source || !$destination) {
            $this->error('Source or destination organisation not found.');
            return Command::FAILURE;
        }

        $response = Http::withToken($source->token->access_token)
            ->withHeaders([
                'Xero-tenant-id' => $source->tenant_id,
                'Accept' => 'application/json',
            ])
            ->get('https://api.xero.com/api.xro/2.0/ContactGroups');

        if (!$response->successful()) {
            $this->error('Failed to fetch contact groups.');
            return Command::FAILURE;
        }

        $groups = collect($response->json('ContactGroups', []));

        if ($groups->isEmpty()) {
            $this->info('No contact groups found.');
            return Command::SUCCESS;
        }

        foreach ($groups as $group) {

            $sourceId = $group['ContactGroupID'];
            $name = trim($group['Name'] ?? '');

            if (!$sourceId || !$name) {
                continue;
            }

            // 🔹 CHECK MAPPING FIRST
            $existing = $destinationRole === 'test'
                ? $mapper->getTestId(self::ENTITY, $sourceId)
                : $mapper->getTargetId(self::ENTITY, $sourceId);

            if ($existing) {
                $destinationId = $existing;
                $this->info("Mapped already → Reusing: {$name} ({$destinationId})");
            } else {

                // 🔹 FALLBACK: FIND BY NAME
                $lookup = Http::withToken($destination->token->access_token)
                    ->withHeaders([
                        'Xero-tenant-id' => $destination->tenant_id,
                        'Accept' => 'application/json',
                    ])
                    ->get('https://api.xero.com/api.xro/2.0/ContactGroups');

                $destinationGroups = collect($lookup->json('ContactGroups', []));

                $match = $destinationGroups->first(function ($g) use ($name) {
                    return mb_strtolower(trim($g['Name'] ?? '')) === mb_strtolower($name);
                });

                if ($match) {
                    $destinationId = $match['ContactGroupID'];
                    $this->info("Matched by name → {$name}");
                } else {

                    $payload = [
                        'ContactGroups' => [
                            ['Name' => $name]
                        ]
                    ];

                    $post = Http::withToken($destination->token->access_token)
                        ->withHeaders([
                            'Xero-tenant-id' => $destination->tenant_id,
                            'Accept' => 'application/json',
                            'Content-Type' => 'application/json',
                        ])
                        ->post('https://api.xero.com/api.xro/2.0/ContactGroups', $payload);

                    if (!$post->successful()) {
                        $this->error("Failed to create {$name}");
                        Log::error('Contact group creation failed', [
                            'payload' => $payload,
                            'response' => $post->body()
                        ]);
                        return Command::FAILURE;
                    }

                    $destinationId = $post->json('ContactGroups.0.ContactGroupID');

                    if (!$destinationId) {
                        $this->error("No destination ID returned.");
                        return Command::FAILURE;
                    }

                    $this->info("Created {$name}");
                }

                // 🔹 STORE + VERIFY
                if ($destinationRole === 'test') {
                    $mapper->storeTest(self::ENTITY, $sourceId, $destinationId, $source->tenant_id, $destination->tenant_id, $name);
                    $stored = $mapper->getTestId(self::ENTITY, $sourceId);
                } else {
                    $mapper->storeTarget(self::ENTITY, $sourceId, $destinationId, $source->tenant_id, $destination->tenant_id, $name);
                    $stored = $mapper->getTargetId(self::ENTITY, $sourceId);
                }

                if (!$stored || $stored !== $destinationId) {
                    $this->error("Mapping verification failed for {$name}");
                    return Command::FAILURE;
                }

                $this->info("Mapped {$name} → {$destinationId}");
            }

            usleep(300000);
        }

        $this->info('Finished.');
        return Command::SUCCESS;
    }
}