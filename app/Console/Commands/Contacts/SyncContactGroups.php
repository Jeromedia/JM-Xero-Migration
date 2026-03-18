<?php

namespace App\Console\Commands\Contacts;

use App\Models\XeroOrganisation;
use App\Services\Xero\XeroIdMapper;
use Illuminate\Console\Command;
use Illuminate\Support\Collection;
use Illuminate\Support\Facades\Http;

class SyncContactGroups extends Command
{
    protected $signature = 'xero:contact-groups:sync {--live : Use target instead of test}';
    protected $description = 'Sync contact groups from SOURCE to TEST or TARGET';

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

        $this->info('Fetching SOURCE contact groups...');

        $sourceResponse = Http::withToken($source->token->access_token)
            ->withHeaders([
                'Xero-tenant-id' => $source->tenant_id,
                'Accept' => 'application/json',
            ])
            ->get('https://api.xero.com/api.xro/2.0/ContactGroups');

        if (!$sourceResponse->successful()) {
            $this->error('Failed to fetch SOURCE contact groups.');
            $this->line($sourceResponse->body());
            return Command::FAILURE;
        }

        $sourceGroups = collect($sourceResponse->json('ContactGroups', []));

        $this->info('Fetching DESTINATION contact groups...');

        $destinationResponse = Http::withToken($destination->token->access_token)
            ->withHeaders([
                'Xero-tenant-id' => $destination->tenant_id,
                'Accept' => 'application/json',
            ])
            ->get('https://api.xero.com/api.xro/2.0/ContactGroups');

        if (!$destinationResponse->successful()) {
            $this->error('Failed to fetch DESTINATION contact groups.');
            $this->line($destinationResponse->body());
            return Command::FAILURE;
        }

        $destinationGroups = collect($destinationResponse->json('ContactGroups', []));

        // Use Name as the practical matching key here.
        // Contact Group names should be unique enough for this initial phase.
        $destinationByName = $destinationGroups->keyBy(function ($group) {
            return mb_strtolower(trim($group['Name'] ?? ''));
        });

        $alreadyMapped = 0;
        $groupsToProcess = collect();

        foreach ($sourceGroups as $group) {
            $sourceGroupId = $group['ContactGroupID'] ?? null;

            if (!$sourceGroupId) {
                continue;
            }

            $mapped = $destinationRole === 'test'
                ? $mapper->getTestId('contact_group', $sourceGroupId)
                : $mapper->getTargetId('contact_group', $sourceGroupId);

            if ($mapped) {
                $alreadyMapped++;
                continue;
            }

            $groupsToProcess->push($group);
        }

        $this->line('');
        $this->info('SOURCE contact groups: ' . $sourceGroups->count());
        $this->info('DESTINATION contact groups: ' . $destinationGroups->count());
        $this->info('Already migrated (mapper): ' . $alreadyMapped);
        $this->info('Contact groups to process: ' . $groupsToProcess->count());
        $this->info('Ready to process: ' . $groupsToProcess->count());

        $this->line('');
        $this->info('Preview (first 10 contact groups):');

        $groupsToProcess->take(10)->each(function ($group) {
            $this->line('- ' . ($group['Name'] ?? '[no name]'));
        });

        $this->line('');

        $mode = $this->choice(
            'Select migration mode',
            [
                'Test (1 contact group only)',
                'Batch (all contact groups)',
            ],
            0
        );

        if ($mode === 'Test (1 contact group only)') {
            $groupsToProcess = $groupsToProcess->take(1);
        }

        if ($groupsToProcess->isEmpty()) {
            $this->info('Nothing to migrate.');
            return Command::SUCCESS;
        }

        if (!$this->confirm('Start processing these contact groups?', true)) {
            return Command::SUCCESS;
        }

        $progress = $this->output->createProgressBar($groupsToProcess->count());
        $progress->start();

        foreach ($groupsToProcess as $group) {
            $sourceGroupId = $group['ContactGroupID'] ?? null;
            $name = trim($group['Name'] ?? '');

            if (!$sourceGroupId || $name === '') {
                $progress->advance();
                continue;
            }

            $destinationKey = mb_strtolower($name);

            if ($destinationByName->has($destinationKey)) {
                // Already exists in destination by name, so just map it.
                $destGroup = $destinationByName[$destinationKey];
                $destinationId = $destGroup['ContactGroupID'] ?? null;

                if (!$destinationId) {
                    $progress->finish();
                    $this->error('');
                    $this->error('Existing destination contact group returned no ContactGroupID.');
                    $this->error($name);
                    $this->line(json_encode($destGroup, JSON_PRETTY_PRINT));

                    return Command::FAILURE;
                }
            } else {
                // Create new contact group in destination.
                // Keep payload minimal.
                $payload = [
                    'ContactGroups' => [
                        [
                            'Name' => $name,
                        ]
                    ]
                ];

                $response = Http::withToken($destination->token->access_token)
                    ->withHeaders([
                        'Xero-tenant-id' => $destination->tenant_id,
                        'Accept' => 'application/json',
                        'Content-Type' => 'application/json',
                    ])
                    ->post(
                        'https://api.xero.com/api.xro/2.0/ContactGroups',
                        $payload
                    );

                if (!$response->successful()) {
                    $progress->finish();
                    $this->error('');
                    $this->error('Contact group creation FAILED.');
                    $this->error($name);
                    $this->line(json_encode($payload, JSON_PRETTY_PRINT));
                    $this->line($response->body());

                    return Command::FAILURE;
                }

                $destinationId = $response->json('ContactGroups.0.ContactGroupID')
                    ?? $response->json('ContactGroupID');

                if (!$destinationId) {
                    $progress->finish();
                    $this->error('');
                    $this->error('Contact group creation returned no destination ContactGroupID.');
                    $this->error($name);
                    $this->line(json_encode($payload, JSON_PRETTY_PRINT));
                    $this->line($response->body());

                    return Command::FAILURE;
                }
            }

            if ($destinationRole === 'test') {
                $mapper->storeTest(
                    'contact_group',
                    $sourceGroupId,
                    $destinationId,
                    $source->tenant_id,
                    $destination->tenant_id,
                    $name
                );
            } else {
                $mapper->storeTarget(
                    'contact_group',
                    $sourceGroupId,
                    $destinationId,
                    $source->tenant_id,
                    $destination->tenant_id
                );
            }

            $progress->advance();
            usleep(200000);
        }

        $progress->finish();

        $this->line('');
        $this->info('Contact groups synchronization completed.');

        return Command::SUCCESS;
    }
}