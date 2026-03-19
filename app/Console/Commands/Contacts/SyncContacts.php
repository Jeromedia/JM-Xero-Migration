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
                            {--first : Sync only the first source contact}';

    protected $description = 'Sync contacts from SOURCE to TEST or TARGET with mapping validation and recovery';

    private const ENTITY = 'contact';

    public function handle(): int
    {
        $destinationRole = $this->option('live') ? 'target' : 'test';

        $this->info('Starting contacts sync (VALIDATED SMART MODE)...');
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

                    if ($mappedContact && $this->isReasonableMappedMatch($sourceContact, $mappedContact)) {
                        $pageMappedValid++;
                        $totalMappedValid++;
                        $this->info("Mapped already → Valid: {$name} ({$mappedId})");

                        if ($stopAfterFirstMatch) {
                            $this->printSummary(
                                $totalProcessed,
                                $totalMappedValid,
                                $totalMappedInvalid,
                                $totalRecovered,
                                $totalCreated
                            );
                            return Command::SUCCESS;
                        }

                        continue;
                    }

                    $pageMappedInvalid++;
                    $totalMappedInvalid++;
                }

                $matchedDestination = $this->findDestinationMatch(
                    $sourceContact,
                    $destinationByName,
                    $destinationByEmail,
                    $destinationByTax
                );

                if ($matchedDestination) {
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
                            $totalCreated
                        );
                        return Command::SUCCESS;
                    }

                    continue;
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
                            $totalCreated
                        );
                        return Command::SUCCESS;
                    }

                    continue;
                }

                $this->warn("Create not completed -> refreshing destination index and retrying recovery: {$name}");

                $destinationContacts = $this->fetchAllContacts($destination, strtoupper($destinationRole));
                [$destinationById, $destinationByName, $destinationByEmail, $destinationByTax] = $this->buildDestinationIndexes($destinationContacts);

                $matchedDestination = $this->findDestinationMatch(
                    $sourceContact,
                    $destinationByName,
                    $destinationByEmail,
                    $destinationByTax
                );

                if ($matchedDestination) {
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
                            $totalCreated
                        );
                        return Command::SUCCESS;
                    }

                    continue;
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
                "Page {$page} summary -> processed: {$pageProcessed} | valid mapped: {$pageMappedValid} | invalid mapped: {$pageMappedInvalid} | recovered: {$pageRecovered} | created: {$pageCreated}"
            );

            $page++;
        }

        $this->printSummary(
            $totalProcessed,
            $totalMappedValid,
            $totalMappedInvalid,
            $totalRecovered,
            $totalCreated
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

        return Http::response([
            'Error' => "Rate limited while creating contact {$name} after retries.",
        ], 429);
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

    private function findDestinationMatch(
        array $sourceContact,
        Collection $destinationByName,
        Collection $destinationByEmail,
        Collection $destinationByTax
    ): ?array {
        $nameKey = $this->normalizeName($sourceContact['Name'] ?? '');
        if ($nameKey !== '') {
            $matches = $destinationByName->get($nameKey);
            if ($matches && $matches->count() === 1) {
                return $matches->first();
            }
        }

        $emailKey = $this->normalizeEmail($sourceContact['EmailAddress'] ?? null);
        if ($emailKey) {
            $matches = $destinationByEmail->get($emailKey);
            if ($matches && $matches->count() === 1) {
                return $matches->first();
            }
        }

        $taxKey = $this->normalizeTax($sourceContact['TaxNumber'] ?? null);
        if ($taxKey) {
            $matches = $destinationByTax->get($taxKey);
            if ($matches && $matches->count() === 1) {
                return $matches->first();
            }
        }

        return null;
    }

    private function isReasonableMappedMatch(array $sourceContact, array $destinationContact): bool
    {
        $sourceName = $this->normalizeName($sourceContact['Name'] ?? '');
        $destinationName = $this->normalizeName($destinationContact['Name'] ?? '');

        if ($sourceName !== '' && $destinationName !== '' && $sourceName === $destinationName) {
            return true;
        }

        $sourceEmail = $this->normalizeEmail($sourceContact['EmailAddress'] ?? null);
        $destinationEmail = $this->normalizeEmail($destinationContact['EmailAddress'] ?? null);

        if ($sourceEmail && $destinationEmail && $sourceEmail === $destinationEmail) {
            return true;
        }

        $sourceTax = $this->normalizeTax($sourceContact['TaxNumber'] ?? null);
        $destinationTax = $this->normalizeTax($destinationContact['TaxNumber'] ?? null);

        if ($sourceTax && $destinationTax && $sourceTax === $destinationTax) {
            return true;
        }

        return false;
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
        int $totalCreated
    ): void {
        $this->newLine();
        $this->info('Done.');
        $this->line("Total processed: {$totalProcessed}");
        $this->line("Valid mapped: {$totalMappedValid}");
        $this->line("Invalid mapped found: {$totalMappedInvalid}");
        $this->line("Recovered + mapped: {$totalRecovered}");
        $this->line("Created + mapped: {$totalCreated}");
    }
}