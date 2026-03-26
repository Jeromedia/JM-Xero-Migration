<?php

namespace App\Console\Commands\Contacts;

use App\Models\XeroOrganisation;
use App\Services\Xero\XeroIdMapper;
use Illuminate\Console\Command;
use Illuminate\Http\Client\Response;
use Illuminate\Support\Collection;
use Illuminate\Support\Facades\Http;
use Illuminate\Support\Facades\Log;

class SyncContacts extends Command
{
    protected $signature = 'xero:contacts:sync
                            {--live : Use target instead of test}
                            {--name= : Sync only one contact by exact Name}
                            {--source-id= : Sync only one contact by source ContactID}
                            {--first : Sync only the first source contact}
                            {--skip-ambiguous : Skip ambiguous contacts instead of failing}';

    protected $description = 'Sync contacts from SOURCE to TEST or TARGET with strict validation, strict recovery, and duplicate protection';

    private const ENTITY = 'contact';

    public function handle(): int
    {
        $destinationRole = $this->option('live') ? 'target' : 'test';

        $this->info('Starting contacts sync (STRICT SAFE MODE)...');
        $this->info('Source role: source');
        $this->info('Destination role: ' . $destinationRole);

        if ($this->option('name')) {
            $this->info('Filter mode: exact name = ' . $this->option('name'));
        }

        if ($this->option('source-id')) {
            $this->info('Filter mode: source ContactID = ' . $this->option('source-id'));
        }

        if ($this->option('first')) {
            $this->info('Filter mode: first source contact only');
        }

        if ($this->option('skip-ambiguous')) {
            $this->info('Ambiguous matches mode: skip');
        } else {
            $this->info('Ambiguous matches mode: fail');
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

        $this->info('Loading destination contacts index...');
        $destinationContacts = $this->fetchAllContacts($destination, strtoupper($destinationRole));
        [$destinationById, $destinationByName, $destinationByEmail, $destinationByTax] = $this->buildDestinationIndexes($destinationContacts);

        $totalProcessed = 0;
        $totalMappedValid = 0;
        $totalMappedInvalid = 0;
        $totalRecovered = 0;
        $totalCreated = 0;
        $totalAmbiguous = 0;
        $totalSkipped = 0;

        $page = 1;
        $stopAfterFirstMatch = false;

        while (true) {
            $response = $this->getContactsPageWithRetry($source, $page, 'SOURCE');
            $sourceContacts = collect($response->json('Contacts', []));

            if ($sourceContacts->isEmpty()) {
                break;
            }

            $sourceContacts = $this->filterSourceContacts($sourceContacts);

            if ($sourceContacts->isEmpty()) {
                $page++;
                continue;
            }

            if ($this->option('first')) {
                $sourceContacts = collect([$sourceContacts->first()]);
                $stopAfterFirstMatch = true;
            }

            $pageProcessed = 0;
            $pageMappedValid = 0;
            $pageMappedInvalid = 0;
            $pageRecovered = 0;
            $pageCreated = 0;
            $pageAmbiguous = 0;
            $pageSkipped = 0;

            $this->info("Processing page {$page} ({$sourceContacts->count()} contacts)...");

            foreach ($sourceContacts as $sourceContact) {
                $pageProcessed++;
                $totalProcessed++;

                $sourceId = $sourceContact['ContactID'] ?? null;
                $name = $sourceContact['Name'] ?? null;

                if (!$sourceId || !$name) {
                    $this->error("Invalid source contact on page {$page}.");
                    $this->line(json_encode($sourceContact, JSON_PRETTY_PRINT | JSON_UNESCAPED_SLASHES));
                    return Command::FAILURE;
                }

                $mappedId = $destinationRole === 'test'
                    ? $mapper->getTestId(self::ENTITY, $sourceId)
                    : $mapper->getTargetId(self::ENTITY, $sourceId);

                if ($mappedId) {
                    $mappedContact = $destinationById->get($mappedId);

                    if ($mappedContact && $this->isStrictMappedMatch($sourceContact, $mappedContact)) {
                        $pageMappedValid++;
                        $totalMappedValid++;
                        $this->info("Mapped already → Valid: {$name} ({$mappedId})");

                        if ($stopAfterFirstMatch) {
                            $this->printSummary(
                                $totalProcessed,
                                $totalMappedValid,
                                $totalMappedInvalid,
                                $totalRecovered,
                                $totalCreated,
                                $totalAmbiguous,
                                $totalSkipped
                            );
                            return Command::SUCCESS;
                        }

                        continue;
                    }

                    $pageMappedInvalid++;
                    $totalMappedInvalid++;

                    Log::warning('Contact mapped ID failed strict validation', [
                        'destination_role' => $destinationRole,
                        'source_contact_id' => $sourceId,
                        'mapped_target_id' => $mappedId,
                        'source_name' => $sourceContact['Name'] ?? null,
                        'source_email' => $sourceContact['EmailAddress'] ?? null,
                        'source_tax' => $sourceContact['TaxNumber'] ?? null,
                        'mapped_name' => $mappedContact['Name'] ?? null,
                        'mapped_email' => $mappedContact['EmailAddress'] ?? null,
                        'mapped_tax' => $mappedContact['TaxNumber'] ?? null,
                    ]);
                }

                $matchResult = $this->findDestinationMatchStrict(
                    $sourceContact,
                    $destinationByName,
                    $destinationByEmail,
                    $destinationByTax
                );

                if ($matchResult['status'] === 'matched') {
                    $matchedDestination = $matchResult['contact'];

                    $this->storeMapping(
                        $mapper,
                        $destinationRole,
                        $source,
                        $destination,
                        $sourceId,
                        $matchedDestination['ContactID'],
                        $name
                    );

                    $verifiedId = $destinationRole === 'test'
                        ? $mapper->getTestId(self::ENTITY, $sourceId)
                        : $mapper->getTargetId(self::ENTITY, $sourceId);

                    if (!$verifiedId || $verifiedId !== $matchedDestination['ContactID']) {
                        $this->error("Mapping verification failed for recovered contact: {$name}");
                        return Command::FAILURE;
                    }

                    $pageRecovered++;
                    $totalRecovered++;
                    $this->info("Recovered + mapped → {$name} ({$matchedDestination['ContactID']})");

                    if ($stopAfterFirstMatch) {
                        $this->printSummary(
                            $totalProcessed,
                            $totalMappedValid,
                            $totalMappedInvalid,
                            $totalRecovered,
                            $totalCreated,
                            $totalAmbiguous,
                            $totalSkipped
                        );
                        return Command::SUCCESS;
                    }

                    continue;
                }

                if ($matchResult['status'] === 'ambiguous') {
                    $pageAmbiguous++;
                    $totalAmbiguous++;

                    $this->warn("Ambiguous destination match -> no create, no map: {$name}");

                    Log::warning('Ambiguous contact match detected', [
                        'destination_role' => $destinationRole,
                        'source_contact_id' => $sourceId,
                        'source_name' => $sourceContact['Name'] ?? null,
                        'source_email' => $sourceContact['EmailAddress'] ?? null,
                        'source_tax' => $sourceContact['TaxNumber'] ?? null,
                        'candidate_count' => count($matchResult['candidates']),
                        'candidates' => $matchResult['candidates'],
                    ]);

                    if ($this->option('skip-ambiguous')) {
                        $pageSkipped++;
                        $totalSkipped++;
                        continue;
                    }

                    $this->error("Ambiguous contact match requires manual review: {$name}");
                    return Command::FAILURE;
                }

                $cleanContact = $this->sanitizeContact($sourceContact);

                if (empty($cleanContact['Name'])) {
                    $this->error("Sanitized contact has no Name: {$name}");
                    $this->line(json_encode($sourceContact, JSON_PRETTY_PRINT | JSON_UNESCAPED_SLASHES));
                    return Command::FAILURE;
                }

                $payload = [
                    'Contacts' => [$cleanContact],
                ];

                $post = $this->postContactsWithRetry($destination, $payload, $name);
                $destinationId = $post->json('Contacts.0.ContactID');

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
                        $this->error("Mapping verification failed for created contact: {$name}");
                        return Command::FAILURE;
                    }

                    $createdContact = $post->json('Contacts.0');
                    if (is_array($createdContact)) {
                        $destinationContacts->push($createdContact);
                        [$destinationById, $destinationByName, $destinationByEmail, $destinationByTax] = $this->buildDestinationIndexes($destinationContacts);
                    }

                    $pageCreated++;
                    $totalCreated++;
                    $this->info("Created + mapped → {$name} ({$destinationId})");

                    if ($stopAfterFirstMatch) {
                        $this->printSummary(
                            $totalProcessed,
                            $totalMappedValid,
                            $totalMappedInvalid,
                            $totalRecovered,
                            $totalCreated,
                            $totalAmbiguous,
                            $totalSkipped
                        );
                        return Command::SUCCESS;
                    }

                    continue;
                }

                $this->warn("Create not completed -> refreshing destination index and retrying strict recovery: {$name}");

                $destinationContacts = $this->fetchAllContacts($destination, strtoupper($destinationRole));
                [$destinationById, $destinationByName, $destinationByEmail, $destinationByTax] = $this->buildDestinationIndexes($destinationContacts);

                $matchResult = $this->findDestinationMatchStrict(
                    $sourceContact,
                    $destinationByName,
                    $destinationByEmail,
                    $destinationByTax
                );

                if ($matchResult['status'] === 'matched') {
                    $matchedDestination = $matchResult['contact'];

                    $this->storeMapping(
                        $mapper,
                        $destinationRole,
                        $source,
                        $destination,
                        $sourceId,
                        $matchedDestination['ContactID'],
                        $name
                    );

                    $verifiedId = $destinationRole === 'test'
                        ? $mapper->getTestId(self::ENTITY, $sourceId)
                        : $mapper->getTargetId(self::ENTITY, $sourceId);

                    if (!$verifiedId || $verifiedId !== $matchedDestination['ContactID']) {
                        $this->error("Mapping verification failed after recovery for: {$name}");
                        return Command::FAILURE;
                    }

                    $pageRecovered++;
                    $totalRecovered++;
                    $this->info("Recovered after refresh + mapped → {$name} ({$matchedDestination['ContactID']})");

                    if ($stopAfterFirstMatch) {
                        $this->printSummary(
                            $totalProcessed,
                            $totalMappedValid,
                            $totalMappedInvalid,
                            $totalRecovered,
                            $totalCreated,
                            $totalAmbiguous,
                            $totalSkipped
                        );
                        return Command::SUCCESS;
                    }

                    continue;
                }

                if ($matchResult['status'] === 'ambiguous') {
                    $pageAmbiguous++;
                    $totalAmbiguous++;

                    $this->warn("Ambiguous destination match after refresh -> no create, no map: {$name}");

                    Log::warning('Ambiguous contact match detected after refresh', [
                        'destination_role' => $destinationRole,
                        'source_contact_id' => $sourceId,
                        'source_name' => $sourceContact['Name'] ?? null,
                        'source_email' => $sourceContact['EmailAddress'] ?? null,
                        'source_tax' => $sourceContact['TaxNumber'] ?? null,
                        'candidate_count' => count($matchResult['candidates']),
                        'candidates' => $matchResult['candidates'],
                        'post_status' => $post->status(),
                        'post_response' => $post->body(),
                    ]);

                    if ($this->option('skip-ambiguous')) {
                        $pageSkipped++;
                        $totalSkipped++;
                        continue;
                    }

                    $this->error("Ambiguous contact match after refresh requires manual review: {$name}");
                    return Command::FAILURE;
                }

                $this->error("Unable to create or recover contact: {$name}");
                $this->line('HTTP Status: ' . $post->status());
                $this->line($post->body());
                $this->line(json_encode($payload, JSON_PRETTY_PRINT | JSON_UNESCAPED_SLASHES));

                Log::error('Contact sync failed', [
                    'destination_role' => $destinationRole,
                    'source_contact_id' => $sourceId,
                    'name' => $name,
                    'payload' => $payload,
                    'status' => $post->status(),
                    'response' => $post->body(),
                ]);

                return Command::FAILURE;
            }

            $this->info(
                "Page {$page} summary -> processed: {$pageProcessed} | valid mapped: {$pageMappedValid} | invalid mapped: {$pageMappedInvalid} | recovered: {$pageRecovered} | created: {$pageCreated} | ambiguous: {$pageAmbiguous} | skipped: {$pageSkipped}"
            );

            $page++;
        }

        $this->printSummary(
            $totalProcessed,
            $totalMappedValid,
            $totalMappedInvalid,
            $totalRecovered,
            $totalCreated,
            $totalAmbiguous,
            $totalSkipped
        );

        return Command::SUCCESS;
    }

    private function fetchAllContacts(XeroOrganisation $organisation, string $label): Collection
    {
        $contacts = collect();
        $page = 1;

        do {
            $response = $this->getContactsPageWithRetry($organisation, $page, $label);

            $data = collect($response->json('Contacts', []));
            $contacts = $contacts->merge($data);
            $page++;
        } while ($data->isNotEmpty());

        return $contacts;
    }

    private function getContactsPageWithRetry(XeroOrganisation $organisation, int $page, string $label): Response
    {
        $maxAttempts = 6;
        $attempt = 1;

        while ($attempt <= $maxAttempts) {
            $response = Http::withToken($organisation->token->access_token)
                ->withHeaders([
                    'Xero-tenant-id' => $organisation->tenant_id,
                    'Accept' => 'application/json',
                ])
                ->get('https://api.xero.com/api.xro/2.0/Contacts', [
                    'page' => $page,
                ]);

            if ($response->successful()) {
                return $response;
            }

            if ($response->status() === 429) {
                $retryAfter = (int) ($response->header('Retry-After') ?? 5);
                $retryAfter = $retryAfter > 0 ? $retryAfter : 5;

                $this->warn("Rate limited while fetching {$label} contacts page {$page}. Waiting {$retryAfter} seconds before retry {$attempt}/{$maxAttempts}...");
                sleep($retryAfter);
                $attempt++;
                continue;
            }

            $this->error("Failed to fetch {$label} contacts on page {$page}.");
            $this->line('HTTP Status: ' . $response->status());
            $this->line($response->body());
            throw new \RuntimeException("Failed to fetch {$label} contacts.");
        }

        $this->error("Failed to fetch {$label} contacts on page {$page} after retries.");
        throw new \RuntimeException("Failed to fetch {$label} contacts after retries.");
    }

    private function postContactsWithRetry(XeroOrganisation $organisation, array $payload, string $name): Response
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
                ->post('https://api.xero.com/api.xro/2.0/Contacts', $payload);

            if ($response->successful()) {
                return $response;
            }

            if ($response->status() === 429) {
                $retryAfter = (int) ($response->header('Retry-After') ?? 5);
                $retryAfter = $retryAfter > 0 ? $retryAfter : 5;

                $this->warn("Rate limited while creating contact {$name}. Waiting {$retryAfter} seconds before retry {$attempt}/{$maxAttempts}...");
                sleep($retryAfter);
                $attempt++;
                continue;
            }

            return $response;
        }

        $this->error("Rate limited while creating contact {$name} after retries.");
        throw new \RuntimeException("Rate limited while creating contact {$name} after retries.");
    }

    private function filterSourceContacts(Collection $contacts): Collection
    {
        if ($this->option('source-id')) {
            $sourceId = trim((string) $this->option('source-id'));

            return $contacts->filter(fn (array $contact) => ($contact['ContactID'] ?? '') === $sourceId)->values();
        }

        if ($this->option('name')) {
            $name = trim((string) $this->option('name'));

            return $contacts->filter(fn (array $contact) => trim((string) ($contact['Name'] ?? '')) === $name)->values();
        }

        return $contacts->values();
    }

    private function buildDestinationIndexes(Collection $contacts): array
    {
        $byId = $contacts
            ->filter(fn ($contact) => !empty($contact['ContactID']))
            ->keyBy('ContactID');

        $byName = $contacts
            ->filter(fn ($contact) => !empty($contact['Name']))
            ->groupBy(fn ($contact) => $this->normalizeName($contact['Name']));

        $byEmail = $contacts
            ->filter(fn ($contact) => !empty($contact['EmailAddress']))
            ->groupBy(fn ($contact) => $this->normalizeEmail($contact['EmailAddress']));

        $byTax = $contacts
            ->filter(fn ($contact) => !empty($contact['TaxNumber']))
            ->groupBy(fn ($contact) => $this->normalizeTax($contact['TaxNumber']));

        return [$byId, $byName, $byEmail, $byTax];
    }

    private function findDestinationMatchStrict(
        array $sourceContact,
        Collection $destinationByName,
        Collection $destinationByEmail,
        Collection $destinationByTax
    ): array {
        $sourceName = $this->normalizeName($sourceContact['Name'] ?? '');
        $sourceEmail = $this->normalizeEmail($sourceContact['EmailAddress'] ?? null);
        $sourceTax = $this->normalizeTax($sourceContact['TaxNumber'] ?? null);

        if ($sourceName === '') {
            return [
                'status' => 'none',
                'contact' => null,
                'candidates' => [],
            ];
        }

        $nameMatches = collect($destinationByName->get($sourceName, collect()))
            ->filter(fn ($contact) => is_array($contact))
            ->values();

        if ($nameMatches->isEmpty()) {
            return [
                'status' => 'none',
                'contact' => null,
                'candidates' => [],
            ];
        }

        $strictMatches = $nameMatches->filter(function (array $destinationContact) use ($sourceEmail, $sourceTax) {
            $destinationEmail = $this->normalizeEmail($destinationContact['EmailAddress'] ?? null);
            $destinationTax = $this->normalizeTax($destinationContact['TaxNumber'] ?? null);

            if ($sourceEmail !== null && $destinationEmail !== $sourceEmail) {
                return false;
            }

            if ($sourceTax !== null && $destinationTax !== $sourceTax) {
                return false;
            }

            return true;
        })->values();

        if ($strictMatches->count() === 1) {
            return [
                'status' => 'matched',
                'contact' => $strictMatches->first(),
                'candidates' => [],
            ];
        }

        if ($strictMatches->count() > 1) {
            return [
                'status' => 'ambiguous',
                'contact' => null,
                'candidates' => $strictMatches
                    ->map(fn (array $contact) => $this->candidateSummary($contact))
                    ->all(),
            ];
        }

        $corroboratedMatches = $nameMatches->filter(function (array $destinationContact) use ($sourceEmail, $sourceTax) {
            $destinationEmail = $this->normalizeEmail($destinationContact['EmailAddress'] ?? null);
            $destinationTax = $this->normalizeTax($destinationContact['TaxNumber'] ?? null);

            $emailMatches = $sourceEmail !== null && $destinationEmail === $sourceEmail;
            $taxMatches = $sourceTax !== null && $destinationTax === $sourceTax;

            return $emailMatches || $taxMatches;
        })->values();

        if ($corroboratedMatches->count() === 1) {
            return [
                'status' => 'matched',
                'contact' => $corroboratedMatches->first(),
                'candidates' => [],
            ];
        }

        if ($corroboratedMatches->count() > 1) {
            return [
                'status' => 'ambiguous',
                'contact' => null,
                'candidates' => $corroboratedMatches
                    ->map(fn (array $contact) => $this->candidateSummary($contact))
                    ->all(),
            ];
        }

        if ($nameMatches->count() === 1) {
            $single = $nameMatches->first();

            $destinationEmail = $this->normalizeEmail($single['EmailAddress'] ?? null);
            $destinationTax = $this->normalizeTax($single['TaxNumber'] ?? null);

            $emailConflict = $sourceEmail !== null && $destinationEmail !== null && $sourceEmail !== $destinationEmail;
            $taxConflict = $sourceTax !== null && $destinationTax !== null && $sourceTax !== $destinationTax;

            if ($emailConflict || $taxConflict) {
                return [
                    'status' => 'ambiguous',
                    'contact' => null,
                    'candidates' => [$this->candidateSummary($single)],
                ];
            }

            return [
                'status' => 'matched',
                'contact' => $single,
                'candidates' => [],
            ];
        }

        return [
            'status' => 'ambiguous',
            'contact' => null,
            'candidates' => $nameMatches
                ->map(fn (array $contact) => $this->candidateSummary($contact))
                ->all(),
        ];
    }

    private function candidateSummary(array $contact): array
    {
        return [
            'ContactID' => $contact['ContactID'] ?? null,
            'Name' => $contact['Name'] ?? null,
            'EmailAddress' => $contact['EmailAddress'] ?? null,
            'TaxNumber' => $contact['TaxNumber'] ?? null,
        ];
    }

    private function isStrictMappedMatch(array $sourceContact, array $destinationContact): bool
    {
        $sourceName = $this->normalizeName($sourceContact['Name'] ?? '');
        $destinationName = $this->normalizeName($destinationContact['Name'] ?? '');

        if ($sourceName === '' || $destinationName === '' || $sourceName !== $destinationName) {
            return false;
        }

        $sourceEmail = $this->normalizeEmail($sourceContact['EmailAddress'] ?? null);
        $destinationEmail = $this->normalizeEmail($destinationContact['EmailAddress'] ?? null);

        if ($sourceEmail !== null && $destinationEmail !== $sourceEmail) {
            return false;
        }

        $sourceTax = $this->normalizeTax($sourceContact['TaxNumber'] ?? null);
        $destinationTax = $this->normalizeTax($destinationContact['TaxNumber'] ?? null);

        if ($sourceTax !== null && $destinationTax !== $sourceTax) {
            return false;
        }

        return true;
    }

    private function normalizeName(?string $value): string
    {
        if (!$value) {
            return '';
        }

        return strtolower(preg_replace('/[^a-z0-9]/i', '', $value));
    }

    private function normalizeEmail(?string $value): ?string
    {
        if (!$value) {
            return null;
        }

        $normalized = strtolower(trim($value));
        return $normalized !== '' ? $normalized : null;
    }

    private function normalizeTax(?string $value): ?string
    {
        if (!$value) {
            return null;
        }

        $normalized = strtolower(preg_replace('/[^a-z0-9]/i', '', $value));
        return $normalized !== '' ? $normalized : null;
    }

    private function sanitizeContact(array $contact): array
    {
        return collect($contact)
            ->except([
                'ContactID',
                'HasAttachments',
                'HasValidationErrors',
                'UpdatedDateUTC',
                'ContactGroups',
                'IsSupplier',
                'IsCustomer',
            ])
            ->map(fn ($value) => $this->sanitizeValue($value))
            ->filter(function ($value) {
                if (is_array($value)) {
                    return !empty($value);
                }

                return $value !== null && $value !== '';
            })
            ->toArray();
    }

    private function sanitizeValue($value)
    {
        if (is_array($value)) {
            $isList = array_keys($value) === range(0, count($value) - 1);

            if ($isList) {
                $cleanedList = [];

                foreach ($value as $item) {
                    $cleanedItem = $this->sanitizeValue($item);

                    if (is_array($cleanedItem) && empty($cleanedItem)) {
                        continue;
                    }

                    if ($cleanedItem === null || $cleanedItem === '') {
                        continue;
                    }

                    $cleanedList[] = $cleanedItem;
                }

                return $cleanedList;
            }

            $cleanedAssoc = [];

            foreach ($value as $key => $item) {
                $cleanedItem = $this->sanitizeValue($item);

                if (is_array($cleanedItem) && empty($cleanedItem)) {
                    continue;
                }

                if ($cleanedItem === null || $cleanedItem === '') {
                    continue;
                }

                $cleanedAssoc[$key] = $cleanedItem;
            }

            return $cleanedAssoc;
        }

        return $value;
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

    private function printSummary(
        int $totalProcessed,
        int $totalMappedValid,
        int $totalMappedInvalid,
        int $totalRecovered,
        int $totalCreated,
        int $totalAmbiguous,
        int $totalSkipped
    ): void {
        $this->newLine();
        $this->info('Done.');
        $this->line("Total processed: {$totalProcessed}");
        $this->line("Valid mapped: {$totalMappedValid}");
        $this->line("Invalid mapped found: {$totalMappedInvalid}");
        $this->line("Recovered + mapped: {$totalRecovered}");
        $this->line("Created + mapped: {$totalCreated}");
        $this->line("Ambiguous found: {$totalAmbiguous}");
        $this->line("Skipped ambiguous: {$totalSkipped}");
    }
}