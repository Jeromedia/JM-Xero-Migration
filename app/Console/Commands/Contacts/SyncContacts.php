<?php

namespace App\Console\Commands\Contacts;

use App\Models\XeroOrganisation;
use App\Services\Xero\XeroIdMapper;
use Illuminate\Console\Command;
use Illuminate\Support\Facades\Http;
use Illuminate\Support\Facades\Log;

class SyncContacts extends Command
{
    protected $signature = 'xero:contacts:sync {--live : Use target instead of test}';
    protected $description = 'Sync contacts from SOURCE to TEST or TARGET';

    private const ENTITY = 'contact';

    public function handle(): int
    {
        $destinationRole = $this->option('live') ? 'target' : 'test';

        $this->info('Starting contacts synchronization...');
        $this->info('Source role: source');
        $this->info('Destination role: ' . $destinationRole);

        $mapper = new XeroIdMapper();

        $mode = $this->choice(
            'Select migration mode',
            [
                'Test (1 contact only)',
                'Batch (100 contacts per page)'
            ],
            0
        );

        $testMode = $mode === 'Test (1 contact only)';

        $source = XeroOrganisation::where('role', 'source')->first();
        $destination = XeroOrganisation::where('role', $destinationRole)->first();

        $page = 1;

        while (true) {

            $this->info("Fetching SOURCE contacts (page {$page})...");

            $response = Http::withToken($source->token->access_token)
                ->withHeaders([
                    'Xero-tenant-id' => $source->tenant_id,
                    'Accept' => 'application/json',
                ])
                ->get('https://api.xero.com/api.xro/2.0/Contacts', [
                    'page' => $page
                ]);

            if (!$response->successful()) {
                $this->error('Failed to fetch contacts.');
                return Command::FAILURE;
            }

            $contacts = collect($response->json('Contacts'));

            if ($contacts->isEmpty()) {
                break;
            }

            if ($testMode) {
                $contacts = $contacts->take(1);
            }

            foreach ($contacts as $contact) {

                $sourceContactId = $contact['ContactID'];

                // 🔹 CHECK MAPPING FIRST
                $existing = $destinationRole === 'test'
                    ? $mapper->getTestId(self::ENTITY, $sourceContactId)
                    : $mapper->getTargetId(self::ENTITY, $sourceContactId);

                if ($existing) {
                    $this->info("Mapped already → Skipping: {$contact['Name']} (ID: {$existing})");
                    continue;
                }

                // 🔹 CLEAN PAYLOAD
                $cleanContact = collect($contact)
                    ->except([
                        'ContactID',
                        'HasAttachments',
                        'HasValidationErrors',
                        'UpdatedDateUTC'
                    ])
                    ->filter(function ($value) {
                        return $value !== null && $value !== '';
                    })
                    ->toArray();

                $payload = [
                    'Contacts' => [$cleanContact]
                ];

                $this->warn("Next contact: {$contact['Name']}");
                $this->line(json_encode($payload, JSON_PRETTY_PRINT));

                if (!$this->confirm('Send this contact now?', false)) {
                    return Command::SUCCESS;
                }

                // 🔹 CREATE CONTACT
                $post = Http::withToken($destination->token->access_token)
                    ->withHeaders([
                        'Xero-tenant-id' => $destination->tenant_id,
                        'Accept' => 'application/json',
                        'Content-Type' => 'application/json'
                    ])
                    ->post('https://api.xero.com/api.xro/2.0/Contacts', $payload);

                if (!$post->successful()) {

                    $this->error("Failed to add {$contact['Name']}");

                    Log::error('Xero contact migration failed', [
                        'payload' => $payload,
                        'response' => $post->body()
                    ]);

                    continue;
                }

                $destinationId = $post->json('Contacts.0.ContactID');

                // 🔴 HARD VALIDATION
                if (!$destinationId) {
                    $this->error("No destination ID returned. Aborting.");
                    return Command::FAILURE;
                }

                // 🔹 STORE MAPPING
                if ($destinationRole === 'test') {

                    $mapper->storeTest(
                        self::ENTITY,
                        $sourceContactId,
                        $destinationId,
                        $source->tenant_id,
                        $destination->tenant_id,
                        $contact['Name']
                    );
                } else {

                    $mapper->storeTarget(
                        self::ENTITY,
                        $sourceContactId,
                        $destinationId,
                        $source->tenant_id,
                        $destination->tenant_id
                    );
                }

                $this->info("Added + mapped {$contact['Name']} → {$destinationId}");

                if ($testMode) {
                    return Command::SUCCESS;
                }

                usleep(500000);
            }

            $page++;
        }

        $this->info('Finished.');
        return Command::SUCCESS;
    }
}
