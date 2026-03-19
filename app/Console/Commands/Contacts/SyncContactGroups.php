<?php

namespace App\Console\Commands\Contacts;

use App\Models\XeroOrganisation;
use App\Services\Xero\XeroIdMapper;
use Illuminate\Console\Command;
use Illuminate\Http\Client\Response;
use Illuminate\Support\Collection;
use Illuminate\Support\Facades\Http;
use Illuminate\Support\Facades\Log;

class SyncContactGroups extends Command
{
    protected $signature = 'xero:contact-groups:sync {--live : Use target instead of test}';
    protected $description = 'Sync contact groups from SOURCE to TEST or TARGET with mapping validation and recovery';

    private const ENTITY = 'contact_group';

    public function handle(): int
    {
        $destinationRole = $this->option('live') ? 'target' : 'test';

        $this->info('Starting contact groups sync (VALIDATED SMART MODE)...');
        $this->info('Source role: source');
        $this->info('Destination role: ' . $destinationRole);

        $mapper = new XeroIdMapper();

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

        $this->info('Loading destination contact groups index...');
        $destinationGroups = $this->fetchAllContactGroups($destination, strtoupper($destinationRole));
        [$destinationById, $destinationByName] = $this->buildDestinationIndexes($destinationGroups);

        $this->info('Fetching SOURCE contact groups...');
        $sourceGroups = $this->fetchAllContactGroups($source, 'SOURCE');

        if ($sourceGroups->isEmpty()) {
            $this->info('No contact groups found.');
            return Command::SUCCESS;
        }

        $processed = 0;
        $mappedValid = 0;
        $mappedInvalid = 0;
        $recovered = 0;
        $created = 0;

        foreach ($sourceGroups as $group) {
            $processed++;

            $sourceId = $group['ContactGroupID'] ?? null;
            $name = trim($group['Name'] ?? '');

            if (!$sourceId || $name === '') {
                $this->error('Invalid source contact group.');
                $this->line(json_encode($group, JSON_PRETTY_PRINT | JSON_UNESCAPED_SLASHES));
                return Command::FAILURE;
            }

            $mappedId = $destinationRole === 'test'
                ? $mapper->getTestId(self::ENTITY, $sourceId)
                : $mapper->getTargetId(self::ENTITY, $sourceId);

            if ($mappedId) {
                $mappedGroup = $destinationById->get($mappedId);

                if ($mappedGroup && $this->isReasonableMappedMatch($group, $mappedGroup)) {
                    $this->info("Mapped already → Valid: {$name} ({$mappedId})");
                    $mappedValid++;
                    continue;
                }

                $mappedInvalid++;
            }

            $matchedDestination = $this->findDestinationMatch($group, $destinationByName);

            if ($matchedDestination) {
                $destinationId = $matchedDestination['ContactGroupID'];

                $this->storeMapping(
                    $mapper,
                    $destinationRole,
                    $source,
                    $destination,
                    $sourceId,
                    $destinationId,
                    $name
                );

                $verifiedId = $destinationRole === 'test'
                    ? $mapper->getTestId(self::ENTITY, $sourceId)
                    : $mapper->getTargetId(self::ENTITY, $sourceId);

                if (!$verifiedId || $verifiedId !== $destinationId) {
                    $this->error("Mapping verification failed for recovered contact group: {$name}");
                    return Command::FAILURE;
                }

                $this->info("Recovered + mapped → {$name} ({$destinationId})");
                $recovered++;
                continue;
            }

            $payload = [
                'ContactGroups' => [
                    ['Name' => $name],
                ],
            ];

            $post = $this->postContactGroupWithRetry($destination, $payload, $name);
            $destinationId = $post->json('ContactGroups.0.ContactGroupID');

            if ($post->successful() && $destinationId) {
                $this->storeMapping(
                    $mapper,
                    $destinationRole,
                    $source,
                    $destination,
                    $sourceId,
                    $destinationId,
                    $name
                );

                $verifiedId = $destinationRole === 'test'
                    ? $mapper->getTestId(self::ENTITY, $sourceId)
                    : $mapper->getTargetId(self::ENTITY, $sourceId);

                if (!$verifiedId || $verifiedId !== $destinationId) {
                    $this->error("Mapping verification failed for created contact group: {$name}");
                    return Command::FAILURE;
                }

                $createdGroup = $post->json('ContactGroups.0');
                if (is_array($createdGroup)) {
                    $destinationGroups->push($createdGroup);
                    [$destinationById, $destinationByName] = $this->buildDestinationIndexes($destinationGroups);
                }

                $this->info("Created + mapped → {$name} ({$destinationId})");
                $created++;
                continue;
            }

            $this->warn("Create not completed -> refreshing destination index and retrying recovery: {$name}");

            $destinationGroups = $this->fetchAllContactGroups($destination, strtoupper($destinationRole));
            [$destinationById, $destinationByName] = $this->buildDestinationIndexes($destinationGroups);

            $matchedDestination = $this->findDestinationMatch($group, $destinationByName);

            if ($matchedDestination) {
                $destinationId = $matchedDestination['ContactGroupID'];

                $this->storeMapping(
                    $mapper,
                    $destinationRole,
                    $source,
                    $destination,
                    $sourceId,
                    $destinationId,
                    $name
                );

                $verifiedId = $destinationRole === 'test'
                    ? $mapper->getTestId(self::ENTITY, $sourceId)
                    : $mapper->getTargetId(self::ENTITY, $sourceId);

                if (!$verifiedId || $verifiedId !== $destinationId) {
                    $this->error("Mapping verification failed after recovery for: {$name}");
                    return Command::FAILURE;
                }

                $this->info("Recovered after refresh + mapped → {$name} ({$destinationId})");
                $recovered++;
                continue;
            }

            $this->error("Unable to create or recover contact group: {$name}");
            $this->line('HTTP Status: ' . $post->status());
            $this->line($post->body());
            $this->line(json_encode($payload, JSON_PRETTY_PRINT | JSON_UNESCAPED_SLASHES));

            Log::error('Contact group sync failed', [
                'name' => $name,
                'payload' => $payload,
                'status' => $post->status(),
                'response' => $post->body(),
            ]);

            return Command::FAILURE;
        }

        $this->newLine();
        $this->info('Done.');
        $this->line("Processed: {$processed}");
        $this->line("Valid mapped: {$mappedValid}");
        $this->line("Invalid mapped found: {$mappedInvalid}");
        $this->line("Recovered + mapped: {$recovered}");
        $this->line("Created + mapped: {$created}");

        return Command::SUCCESS;
    }

    private function fetchAllContactGroups(XeroOrganisation $organisation, string $label): Collection
    {
        $response = $this->getContactGroupsWithRetry($organisation, $label);

        return collect($response->json('ContactGroups', []));
    }

    private function getContactGroupsWithRetry(XeroOrganisation $organisation, string $label): Response
    {
        $maxAttempts = 6;
        $attempt = 1;

        while ($attempt <= $maxAttempts) {
            $response = Http::withToken($organisation->token->access_token)
                ->withHeaders([
                    'Xero-tenant-id' => $organisation->tenant_id,
                    'Accept' => 'application/json',
                ])
                ->get('https://api.xero.com/api.xro/2.0/ContactGroups');

            if ($response->successful()) {
                return $response;
            }

            if ($response->status() === 429) {
                $retryAfter = (int) ($response->header('Retry-After') ?? 5);
                $retryAfter = $retryAfter > 0 ? $retryAfter : 5;

                $this->warn("Rate limited while fetching {$label} contact groups. Waiting {$retryAfter} seconds before retry {$attempt}/{$maxAttempts}...");
                sleep($retryAfter);
                $attempt++;
                continue;
            }

            $this->error("Failed to fetch {$label} contact groups.");
            $this->line('HTTP Status: ' . $response->status());
            $this->line($response->body());
            throw new \RuntimeException("Failed to fetch {$label} contact groups.");
        }

        $this->error("Failed to fetch {$label} contact groups after retries.");
        throw new \RuntimeException("Failed to fetch {$label} contact groups after retries.");
    }

    private function postContactGroupWithRetry(XeroOrganisation $organisation, array $payload, string $name): Response
    {
        $maxAttempts = 6;
        $attempt = 1;

        while ($attempt <= $maxAttempts) {
            $response = Http::withToken($organisation->token->access_token)
                ->withHeaders([
                    'Xero-tenant-id' => $organisation->tenant_id,
                    'Accept' => 'application/json',
                    'Content-Type' => 'application/json',
                ])
                ->post('https://api.xero.com/api.xro/2.0/ContactGroups', $payload);

            if ($response->successful()) {
                return $response;
            }

            if ($response->status() === 429) {
                $retryAfter = (int) ($response->header('Retry-After') ?? 5);
                $retryAfter = $retryAfter > 0 ? $retryAfter : 5;

                $this->warn("Rate limited while creating contact group {$name}. Waiting {$retryAfter} seconds before retry {$attempt}/{$maxAttempts}...");
                sleep($retryAfter);
                $attempt++;
                continue;
            }

            return $response;
        }

        return Http::response([
            'Error' => "Rate limited while creating contact group {$name} after retries.",
        ], 429);
    }

    private function buildDestinationIndexes(Collection $groups): array
    {
        $byId = $groups
            ->filter(fn ($group) => !empty($group['ContactGroupID']))
            ->keyBy('ContactGroupID');

        $byName = $groups
            ->filter(fn ($group) => !empty($group['Name']))
            ->groupBy(fn ($group) => $this->normalizeName($group['Name']));

        return [$byId, $byName];
    }

    private function findDestinationMatch(array $sourceGroup, Collection $destinationByName): ?array
    {
        $nameKey = $this->normalizeName($sourceGroup['Name'] ?? '');

        if ($nameKey === '') {
            return null;
        }

        $matches = $destinationByName->get($nameKey);

        if ($matches && $matches->count() === 1) {
            return $matches->first();
        }

        return null;
    }

    private function isReasonableMappedMatch(array $sourceGroup, array $destinationGroup): bool
    {
        $sourceName = $this->normalizeName($sourceGroup['Name'] ?? '');
        $destinationName = $this->normalizeName($destinationGroup['Name'] ?? '');

        return $sourceName !== '' && $destinationName !== '' && $sourceName === $destinationName;
    }

    private function normalizeName(?string $value): string
    {
        if (!$value) {
            return '';
        }

        return mb_strtolower(trim($value));
    }

    private function storeMapping(
        XeroIdMapper $mapper,
        string $destinationRole,
        XeroOrganisation $source,
        XeroOrganisation $destination,
        string $sourceId,
        string $destinationId,
        string $name
    ): void {
        if ($destinationRole === 'test') {
            $mapper->storeTest(
                self::ENTITY,
                $sourceId,
                $destinationId,
                $source->tenant_id,
                $destination->tenant_id,
                $name
            );
            return;
        }

        $mapper->storeTarget(
            self::ENTITY,
            $sourceId,
            $destinationId,
            $source->tenant_id,
            $destination->tenant_id,
            $name
        );
    }
}