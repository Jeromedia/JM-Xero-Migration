<?php

namespace App\Console\Commands\Contacts;

use App\Models\XeroOrganisation;
use App\Services\Xero\XeroIdMapper;
use Illuminate\Console\Command;
use Illuminate\Http\Client\Response;
use Illuminate\Support\Collection;
use Illuminate\Support\Facades\Http;
use Illuminate\Support\Facades\Log;
use Illuminate\Support\Str;

class SyncContactGroupMembers extends Command
{
    protected $signature = 'xero:contact-group-members:sync
                            {--live : Use target instead of test}
                            {--group-id= : Sync only one exact source group ID}
                            {--group-name= : Sync only one exact source group name}
                            {--dry-run : Show what would be sent without writing to Xero}';

    protected $description = 'Sync contact memberships from SOURCE groups into mapped TEST or TARGET groups';

    private const GROUP_ENTITY = 'contact_group';
    private const CONTACT_ENTITY = 'contact';

    public function handle(): int
    {
        $destinationRole = $this->option('live') ? 'target' : 'test';
        $dryRun = (bool) $this->option('dry-run');
        $groupIdFilter = trim((string) $this->option('group-id'));
        $groupNameFilter = trim((string) $this->option('group-name'));

        $this->info('Starting contact group members sync (VALIDATED SMART MODE)...');
        $this->info('Source role: source');
        $this->info('Destination role: ' . $destinationRole);
        $this->info('Mode: ' . ($dryRun ? 'DRY RUN' : 'LIVE WRITE'));

        if ($groupIdFilter !== '') {
            $this->info('Filter mode: exact group ID = ' . $groupIdFilter);
        }

        if ($groupNameFilter !== '') {
            $this->info('Filter mode: exact group name = ' . $groupNameFilter);
        }

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
        [$destinationGroupsById, $_destinationGroupsByName] = $this->buildGroupIndexes($destinationGroups);

        $this->info('Fetching SOURCE contact groups...');
        $sourceGroups = $this->fetchAllContactGroups($source, 'SOURCE');

        if ($groupIdFilter !== '') {
            $sourceGroups = $sourceGroups->filter(function (array $group) use ($groupIdFilter) {
                return ($group['ContactGroupID'] ?? '') === $groupIdFilter;
            })->values();
        }

        if ($groupNameFilter !== '') {
            $normalizedFilter = $this->normalizeName($groupNameFilter);
            $sourceGroups = $sourceGroups->filter(function (array $group) use ($normalizedFilter) {
                return $this->normalizeName($group['Name'] ?? '') === $normalizedFilter;
            })->values();
        }

        if ($sourceGroups->isEmpty()) {
            $this->info('No source contact groups found.');
            return Command::SUCCESS;
        }

        $processedGroups = 0;
        $groupsReady = 0;
        $groupsUpdated = 0;
        $groupsSkippedNoGroupMapping = 0;
        $groupsSkippedMissingTargetGroup = 0;
        $groupsSkippedNoSourceContacts = 0;
        $groupsSkippedNoMappedContacts = 0;
        $groupsSkippedAlreadyAligned = 0;
        $contactsPrepared = 0;
        $contactsSkippedNoContactMapping = 0;
        $contactsDedupedFromSource = 0;
        $contactsAlreadyInTargetGroup = 0;
        $sourceGroupDetailFetches = 0;
        $targetGroupDetailFetches = 0;

        foreach ($sourceGroups as $group) {
            $processedGroups++;

            $sourceGroupId = $group['ContactGroupID'] ?? null;
            $sourceGroupName = trim((string) ($group['Name'] ?? ''));

            if (!$sourceGroupId || $sourceGroupName === '') {
                $this->error('Invalid source contact group.');
                $this->line(json_encode($group, JSON_PRETTY_PRINT | JSON_UNESCAPED_SLASHES));
                return Command::FAILURE;
            }

            $targetGroupId = $destinationRole === 'test'
                ? $mapper->getTestId(self::GROUP_ENTITY, $sourceGroupId)
                : $mapper->getTargetId(self::GROUP_ENTITY, $sourceGroupId);

            if (!$targetGroupId) {
                $this->warn("Skipped group (no group mapping) → {$sourceGroupName}");
                $groupsSkippedNoGroupMapping++;
                continue;
            }

            $targetGroup = $destinationGroupsById->get($targetGroupId);

            if (!$targetGroup) {
                $this->warn("Skipped group (mapped target group not found in {$destinationRole}) → {$sourceGroupName} ({$targetGroupId})");
                $groupsSkippedMissingTargetGroup++;
                continue;
            }

            $sourceContacts = collect($group['Contacts'] ?? []);

            if ($sourceContacts->isEmpty()) {
                $sourceGroupDetail = $this->fetchSingleContactGroup($source, 'SOURCE', $sourceGroupId);
                $sourceGroupDetailFetches++;
                $sourceContacts = collect($sourceGroupDetail['Contacts'] ?? []);
            }

            if ($sourceContacts->isEmpty()) {
                $this->line("No source contacts in group → {$sourceGroupName}");
                $groupsSkippedNoSourceContacts++;
                continue;
            }

            $targetGroupDetailFetches++;
            $targetGroupDetail = $this->fetchSingleContactGroup($destination, strtoupper($destinationRole), $targetGroupId);

            $existingTargetContactIds = collect($targetGroupDetail['Contacts'] ?? [])
                ->pluck('ContactID')
                ->filter()
                ->map(fn ($id) => (string) $id)
                ->unique()
                ->values()
                ->all();

            $existingTargetContactLookup = array_fill_keys($existingTargetContactIds, true);

            $contactsToAdd = [];
            $seenTargetContactIdsInPayload = [];

            foreach ($sourceContacts as $sourceContact) {
                $sourceContactId = $sourceContact['ContactID'] ?? null;
                $sourceContactName = trim((string) ($sourceContact['Name'] ?? $sourceContactId ?? ''));

                if (!$sourceContactId) {
                    continue;
                }

                $mappedTargetContactId = $destinationRole === 'test'
                    ? $mapper->getTestId(self::CONTACT_ENTITY, $sourceContactId)
                    : $mapper->getTargetId(self::CONTACT_ENTITY, $sourceContactId);

                if (!$mappedTargetContactId) {
                    $contactsSkippedNoContactMapping++;
                    $this->warn("Skipped contact (no contact mapping) → {$sourceContactName} in group {$sourceGroupName}");
                    continue;
                }

                $mappedTargetContactId = (string) $mappedTargetContactId;

                if (isset($existingTargetContactLookup[$mappedTargetContactId])) {
                    $contactsAlreadyInTargetGroup++;
                    continue;
                }

                if (isset($seenTargetContactIdsInPayload[$mappedTargetContactId])) {
                    $contactsDedupedFromSource++;
                    continue;
                }

                $seenTargetContactIdsInPayload[$mappedTargetContactId] = true;
                $contactsToAdd[] = [
                    'ContactID' => $mappedTargetContactId,
                ];
            }

            if (empty($contactsToAdd)) {
                $this->line("Already aligned / nothing to add → {$sourceGroupName}");
                $groupsSkippedAlreadyAligned++;
                $groupsSkippedNoMappedContacts++;
                continue;
            }

            $groupsReady++;
            $contactsPrepared += count($contactsToAdd);

            $payload = [
                'Contacts' => $contactsToAdd,
            ];

            if ($dryRun) {
                $this->info("DRY RUN → {$sourceGroupName} ({$targetGroupId}) would add " . count($contactsToAdd) . ' contacts');
                continue;
            }

            $idempotencyKey = (string) Str::uuid();

            $put = $this->putGroupContactsWithRetry(
                $destination,
                $targetGroupId,
                $payload,
                $sourceGroupName,
                $idempotencyKey
            );

            if ($put->successful()) {
                $this->info("Updated group → {$sourceGroupName} ({$targetGroupId}) with " . count($contactsToAdd) . ' contacts');
                $groupsUpdated++;
                continue;
            }

            $this->error("Unable to update group members: {$sourceGroupName}");
            $this->line('HTTP Status: ' . $put->status());
            $this->line($put->body());
            $this->line(json_encode($payload, JSON_PRETTY_PRINT | JSON_UNESCAPED_SLASHES));

            Log::error('Contact group members sync failed', [
                'group_name' => $sourceGroupName,
                'source_group_id' => $sourceGroupId,
                'target_group_id' => $targetGroupId,
                'payload' => $payload,
                'status' => $put->status(),
                'response' => $put->body(),
            ]);

            return Command::FAILURE;
        }

        $this->newLine();
        $this->info('Done.');
        $this->line("Processed groups: {$processedGroups}");
        $this->line("Groups ready to update: {$groupsReady}");
        $this->line("Groups updated: {$groupsUpdated}");
        $this->line("Groups skipped (no group mapping): {$groupsSkippedNoGroupMapping}");
        $this->line("Groups skipped (mapped target group missing): {$groupsSkippedMissingTargetGroup}");
        $this->line("Groups skipped (no source contacts): {$groupsSkippedNoSourceContacts}");
        $this->line("Groups skipped (already aligned): {$groupsSkippedAlreadyAligned}");
        $this->line("Groups skipped (no mapped contacts to add): {$groupsSkippedNoMappedContacts}");
        $this->line("Contacts prepared to add: {$contactsPrepared}");
        $this->line("Contacts skipped (no contact mapping): {$contactsSkippedNoContactMapping}");
        $this->line("Contacts already in target group: {$contactsAlreadyInTargetGroup}");
        $this->line("Contacts deduped from source: {$contactsDedupedFromSource}");
        $this->line("Extra source group detail fetches: {$sourceGroupDetailFetches}");
        $this->line("Extra target group detail fetches: {$targetGroupDetailFetches}");

        return Command::SUCCESS;
    }

    private function fetchAllContactGroups(XeroOrganisation $organisation, string $label): Collection
    {
        $response = $this->getContactGroupsWithRetry($organisation, $label);

        return collect($response->json('ContactGroups', []));
    }

    private function fetchSingleContactGroup(XeroOrganisation $organisation, string $label, string $contactGroupId): array
    {
        $response = $this->getSingleContactGroupWithRetry($organisation, $label, $contactGroupId);

        $group = $response->json('ContactGroups.0');

        return is_array($group) ? $group : [];
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

    private function getSingleContactGroupWithRetry(
        XeroOrganisation $organisation,
        string $label,
        string $contactGroupId
    ): Response {
        $maxAttempts = 6;
        $attempt = 1;

        while ($attempt <= $maxAttempts) {
            $response = Http::withToken($organisation->token->access_token)
                ->withHeaders([
                    'Xero-tenant-id' => $organisation->tenant_id,
                    'Accept' => 'application/json',
                ])
                ->get("https://api.xero.com/api.xro/2.0/ContactGroups/{$contactGroupId}");

            if ($response->successful()) {
                return $response;
            }

            if ($response->status() === 429) {
                $retryAfter = (int) ($response->header('Retry-After') ?? 5);
                $retryAfter = $retryAfter > 0 ? $retryAfter : 5;

                $this->warn("Rate limited while fetching {$label} contact group {$contactGroupId}. Waiting {$retryAfter} seconds before retry {$attempt}/{$maxAttempts}...");
                sleep($retryAfter);
                $attempt++;
                continue;
            }

            $this->error("Failed to fetch {$label} contact group {$contactGroupId}.");
            $this->line('HTTP Status: ' . $response->status());
            $this->line($response->body());
            throw new \RuntimeException("Failed to fetch {$label} contact group {$contactGroupId}.");
        }

        $this->error("Failed to fetch {$label} contact group {$contactGroupId} after retries.");
        throw new \RuntimeException("Failed to fetch {$label} contact group {$contactGroupId} after retries.");
    }

    private function putGroupContactsWithRetry(
        XeroOrganisation $organisation,
        string $targetGroupId,
        array $payload,
        string $groupName,
        string $idempotencyKey
    ): Response {
        $maxAttempts = 6;
        $attempt = 1;

        while ($attempt <= $maxAttempts) {
            $response = Http::withToken($organisation->token->access_token)
                ->withHeaders([
                    'Xero-tenant-id' => $organisation->tenant_id,
                    'Accept' => 'application/json',
                    'Content-Type' => 'application/json',
                    'Idempotency-Key' => $idempotencyKey,
                ])
                ->put("https://api.xero.com/api.xro/2.0/ContactGroups/{$targetGroupId}/Contacts", $payload);

            if ($response->successful()) {
                return $response;
            }

            if ($response->status() === 429) {
                $retryAfter = (int) ($response->header('Retry-After') ?? 5);
                $retryAfter = $retryAfter > 0 ? $retryAfter : 5;

                $this->warn("Rate limited while updating group {$groupName}. Waiting {$retryAfter} seconds before retry {$attempt}/{$maxAttempts}...");
                sleep($retryAfter);
                $attempt++;
                continue;
            }

            return $response;
        }

        throw new \RuntimeException("Rate limited while updating group {$groupName} after retries.");
    }

    private function buildGroupIndexes(Collection $groups): array
    {
        $byId = $groups
            ->filter(fn ($group) => !empty($group['ContactGroupID']))
            ->keyBy('ContactGroupID');

        $byName = $groups
            ->filter(fn ($group) => !empty($group['Name']))
            ->groupBy(fn ($group) => $this->normalizeName($group['Name']));

        return [$byId, $byName];
    }

    private function normalizeName(?string $value): string
    {
        if (!$value) {
            return '';
        }

        $value = preg_replace('/\s+/', ' ', trim($value));

        return mb_strtolower($value ?? '');
    }
}