<?php

namespace App\Console\Commands\Contacts;

use App\Models\XeroOrganisation;
use App\Services\Xero\XeroIdMapper;
use Illuminate\Console\Command;
use Illuminate\Support\Facades\Http;

class SyncContactGroupMemberships extends Command
{
    protected $signature = 'xero:contact-group-memberships:sync {--live : Use target instead of test}';
    protected $description = 'Assign migrated contacts to migrated contact groups in TEST or TARGET';

    public function handle(): int
    {
        $destinationRole = $this->option('live') ? 'target' : 'test';

        $this->info('Starting contact group membership synchronization...');
        $this->info('Source role: source');
        $this->info('Destination role: ' . $destinationRole);

        $mapper = new XeroIdMapper();

        $source = XeroOrganisation::where('role', 'source')->first();
        $destination = XeroOrganisation::where('role', $destinationRole)->first();

        if (!$source || !$destination) {
            $this->error('Source or destination organisation not found.');
            return Command::FAILURE;
        }

        $page = 1;
        $processed = 0;

        while (true) {
            $this->info("Fetching SOURCE contacts (page {$page})...");

            $response = Http::withToken($source->token->access_token)
                ->withHeaders([
                    'Xero-tenant-id' => $source->tenant_id,
                    'Accept' => 'application/json',
                ])
                ->get('https://api.xero.com/api.xro/2.0/Contacts', [
                    'page' => $page,
                ]);

            if (!$response->successful()) {
                $this->error('Failed to fetch SOURCE contacts.');
                $this->line($response->body());
                return Command::FAILURE;
            }

            $contacts = collect($response->json('Contacts', []));

            if ($contacts->isEmpty()) {
                break;
            }

            foreach ($contacts as $contact) {
                $sourceContactId = $contact['ContactID'] ?? null;
                $contactName = $contact['Name'] ?? '[no name]';
                $sourceGroups = collect($contact['ContactGroups'] ?? []);

                if (!$sourceContactId || $sourceGroups->isEmpty()) {
                    continue;
                }

                $destinationContactId = $destinationRole === 'test'
                    ? $mapper->getTestId('contact', $sourceContactId)
                    : $mapper->getTargetId('contact', $sourceContactId);

                if (!$destinationContactId) {
                    $this->warn("Skipping {$contactName} - no mapped destination contact ID.");
                    continue;
                }

                foreach ($sourceGroups as $group) {
                    $sourceGroupId = $group['ContactGroupID'] ?? null;
                    $groupName = $group['Name'] ?? '[no group name]';

                    if (!$sourceGroupId) {
                        continue;
                    }

                    $destinationGroupId = $destinationRole === 'test'
                        ? $mapper->getTestId('contact_group', $sourceGroupId)
                        : $mapper->getTargetId('contact_group', $sourceGroupId);

                    if (!$destinationGroupId) {
                        $this->warn("Skipping group {$groupName} for {$contactName} - no mapped destination group ID.");
                        continue;
                    }

                    $payload = [
                        'Contacts' => [
                            [
                                'ContactID' => $destinationContactId,
                            ]
                        ]
                    ];

                    $this->line("Assigning {$contactName} to group {$groupName}...");

                    $post = Http::withToken($destination->token->access_token)
                        ->withHeaders([
                            'Xero-tenant-id' => $destination->tenant_id,
                            'Accept' => 'application/json',
                            'Content-Type' => 'application/json',
                        ])
                        ->put(
                            'https://api.xero.com/api.xro/2.0/ContactGroups/' . $destinationGroupId . '/Contacts',
                            $payload
                        );

                    if (!$post->successful()) {
                        $this->error("Failed to assign {$contactName} to {$groupName}");
                        $this->line(json_encode($payload, JSON_PRETTY_PRINT));
                        $this->line($post->body());
                        return Command::FAILURE;
                    }

                    $processed++;
                    usleep(200000);
                }
            }

            $page++;
        }

        $this->info("Finished. Membership assignments processed: {$processed}");
        return Command::SUCCESS;
    }
}