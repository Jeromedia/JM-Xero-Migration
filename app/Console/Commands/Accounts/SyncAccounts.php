<?php

namespace App\Console\Commands\Accounts;

use App\Models\XeroOrganisation;
use App\Services\Xero\XeroIdMapper;
use Illuminate\Console\Command;
use Illuminate\Support\Collection;
use Illuminate\Support\Facades\Http;

class SyncAccounts extends Command
{
    protected $signature = 'xero:accounts:sync {--live : Use target instead of test}';
    protected $description = 'Sync chart of accounts from SOURCE to TEST or TARGET with mapping validation and recovery';

    private const ENTITY = 'account';

    public function handle(): int
    {
        $destinationRole = $this->option('live') ? 'target' : 'test';

        $this->info('Starting accounts sync (VALIDATED SMART MODE)...');
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

        $this->info('Loading destination accounts index...');
        $destinationAccounts = $this->fetchAllAccounts($destination, strtoupper($destinationRole));
        [$destinationById, $destinationByCode, $destinationByName] = $this->buildDestinationIndexes($destinationAccounts);

        $totalProcessed = 0;
        $totalMappedValid = 0;
        $totalMappedInvalid = 0;
        $totalRecovered = 0;
        $totalCreated = 0;

        $page = 1;

        while (true) {
            $response = Http::withToken($source->token->access_token)
                ->withHeaders([
                    'Xero-tenant-id' => $source->tenant_id,
                    'Accept' => 'application/json',
                ])
                ->get('https://api.xero.com/api.xro/2.0/Accounts', [
                    'page' => $page,
                ]);

            if (!$response->successful()) {
                $this->error("Failed to fetch SOURCE accounts on page {$page}.");
                $this->line('HTTP Status: ' . $response->status());
                $this->line($response->body());
                return Command::FAILURE;
            }

            $sourceAccounts = collect($response->json('Accounts', []));

            if ($sourceAccounts->isEmpty()) {
                break;
            }

            $pageProcessed = 0;
            $pageMappedValid = 0;
            $pageMappedInvalid = 0;
            $pageRecovered = 0;
            $pageCreated = 0;

            $this->info("Processing page {$page} ({$sourceAccounts->count()} accounts)...");

            foreach ($sourceAccounts as $sourceAccount) {
                $pageProcessed++;
                $totalProcessed++;

                $sourceId = $sourceAccount['AccountID'] ?? null;
                $code = $sourceAccount['Code'] ?? null;
                $name = $sourceAccount['Name'] ?? null;

                if (!$sourceId || !$code || !$name) {
                    $this->error("Invalid source account on page {$page}.");
                    $this->line(json_encode($sourceAccount, JSON_PRETTY_PRINT | JSON_UNESCAPED_SLASHES));
                    return Command::FAILURE;
                }

                $mappedId = $destinationRole === 'test'
                    ? $mapper->getTestId(self::ENTITY, $sourceId)
                    : $mapper->getTargetId(self::ENTITY, $sourceId);

                if ($mappedId) {
                    $mappedAccount = $destinationById->get($mappedId);

                    if ($mappedAccount && $this->isReasonableMappedMatch($sourceAccount, $mappedAccount)) {
                        $pageMappedValid++;
                        $totalMappedValid++;
                        continue;
                    }

                    $pageMappedInvalid++;
                    $totalMappedInvalid++;
                }

                $matchedDestination = $this->findDestinationMatch(
                    $sourceAccount,
                    $destinationByCode,
                    $destinationByName
                );

                if ($matchedDestination) {
                    $this->storeMapping(
                        $mapper,
                        $destinationRole,
                        $source,
                        $destination,
                        $sourceId,
                        $matchedDestination['AccountID'],
                        $name
                    );

                    $verifiedId = $destinationRole === 'test'
                        ? $mapper->getTestId(self::ENTITY, $sourceId)
                        : $mapper->getTargetId(self::ENTITY, $sourceId);

                    if (!$verifiedId || $verifiedId !== $matchedDestination['AccountID']) {
                        $this->error("Mapping verification failed for recovered account: {$code} - {$name}");
                        return Command::FAILURE;
                    }

                    $pageRecovered++;
                    $totalRecovered++;
                    continue;
                }

                $cleanAccount = $this->sanitizeAccount($sourceAccount);

                if (empty($cleanAccount['Code']) || empty($cleanAccount['Name']) || empty($cleanAccount['Type'])) {
                    $this->error("Sanitized account is missing Code, Name or Type: {$code} - {$name}");
                    $this->line(json_encode($sourceAccount, JSON_PRETTY_PRINT | JSON_UNESCAPED_SLASHES));
                    return Command::FAILURE;
                }

                $payload = [
                    'Accounts' => [$cleanAccount],
                ];

                $put = Http::withToken($destination->token->access_token)
                    ->withHeaders([
                        'Xero-tenant-id' => $destination->tenant_id,
                        'Accept' => 'application/json',
                        'Content-Type' => 'application/json',
                    ])
                    ->put('https://api.xero.com/api.xro/2.0/Accounts', $payload);

                $destinationId = $put->json('Accounts.0.AccountID');

                if ($put->successful() && $destinationId) {
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
                        $this->error("Mapping verification failed for created account: {$code} - {$name}");
                        return Command::FAILURE;
                    }

                    $createdAccount = $put->json('Accounts.0');
                    if (is_array($createdAccount)) {
                        $destinationAccounts->push($createdAccount);
                        [$destinationById, $destinationByCode, $destinationByName] = $this->buildDestinationIndexes($destinationAccounts);
                    }

                    $pageCreated++;
                    $totalCreated++;
                    continue;
                }

                $this->warn("Create not completed -> refreshing destination index and retrying recovery: {$code} - {$name}");

                $destinationAccounts = $this->fetchAllAccounts($destination, strtoupper($destinationRole));
                [$destinationById, $destinationByCode, $destinationByName] = $this->buildDestinationIndexes($destinationAccounts);

                $matchedDestination = $this->findDestinationMatch(
                    $sourceAccount,
                    $destinationByCode,
                    $destinationByName
                );

                if ($matchedDestination) {
                    $this->storeMapping(
                        $mapper,
                        $destinationRole,
                        $source,
                        $destination,
                        $sourceId,
                        $matchedDestination['AccountID'],
                        $name
                    );

                    $verifiedId = $destinationRole === 'test'
                        ? $mapper->getTestId(self::ENTITY, $sourceId)
                        : $mapper->getTargetId(self::ENTITY, $sourceId);

                    if (!$verifiedId || $verifiedId !== $matchedDestination['AccountID']) {
                        $this->error("Mapping verification failed after recovery for: {$code} - {$name}");
                        return Command::FAILURE;
                    }

                    $pageRecovered++;
                    $totalRecovered++;
                    continue;
                }

                $this->error("Unable to create or recover account: {$code} - {$name}");
                $this->line('HTTP Status: ' . $put->status());
                $this->line($put->body());
                $this->line(json_encode($payload, JSON_PRETTY_PRINT | JSON_UNESCAPED_SLASHES));

                return Command::FAILURE;
            }

            $this->info(
                "Page {$page} summary -> processed: {$pageProcessed} | valid mapped: {$pageMappedValid} | invalid mapped: {$pageMappedInvalid} | recovered: {$pageRecovered} | created: {$pageCreated}"
            );

            $page++;
        }

        $this->newLine();
        $this->info('Done.');
        $this->line("Total processed: {$totalProcessed}");
        $this->line("Valid mapped: {$totalMappedValid}");
        $this->line("Invalid mapped found: {$totalMappedInvalid}");
        $this->line("Recovered + mapped: {$totalRecovered}");
        $this->line("Created + mapped: {$totalCreated}");

        return Command::SUCCESS;
    }

    private function fetchAllAccounts(XeroOrganisation $organisation, string $label): Collection
    {
        $accounts = collect();
        $page = 1;

        do {
            $response = Http::withToken($organisation->token->access_token)
                ->withHeaders([
                    'Xero-tenant-id' => $organisation->tenant_id,
                    'Accept' => 'application/json',
                ])
                ->get('https://api.xero.com/api.xro/2.0/Accounts', [
                    'page' => $page,
                ]);

            if (!$response->successful()) {
                $this->error("Failed to fetch {$label} accounts.");
                $this->line('HTTP Status: ' . $response->status());
                $this->line($response->body());
                throw new \RuntimeException("Failed to fetch {$label} accounts.");
            }

            $data = collect($response->json('Accounts', []));
            $accounts = $accounts->merge($data);
            $page++;
        } while ($data->isNotEmpty());

        return $accounts;
    }

    private function buildDestinationIndexes(Collection $accounts): array
    {
        $byId = $accounts
            ->filter(fn ($account) => !empty($account['AccountID']))
            ->keyBy('AccountID');

        $byCode = $accounts
            ->filter(fn ($account) => !empty($account['Code']))
            ->groupBy(fn ($account) => $this->normalizeCode($account['Code']));

        $byName = $accounts
            ->filter(fn ($account) => !empty($account['Name']))
            ->groupBy(fn ($account) => $this->normalizeName($account['Name']));

        return [$byId, $byCode, $byName];
    }

    private function findDestinationMatch(
        array $sourceAccount,
        Collection $destinationByCode,
        Collection $destinationByName
    ): ?array {
        $codeKey = $this->normalizeCode($sourceAccount['Code'] ?? null);
        if ($codeKey) {
            $matches = $destinationByCode->get($codeKey);
            if ($matches && $matches->count() === 1) {
                return $matches->first();
            }
        }

        $nameKey = $this->normalizeName($sourceAccount['Name'] ?? '');
        if ($nameKey !== '') {
            $matches = $destinationByName->get($nameKey);
            if ($matches && $matches->count() === 1) {
                return $matches->first();
            }
        }

        return null;
    }

    private function isReasonableMappedMatch(array $sourceAccount, array $destinationAccount): bool
    {
        $sourceCode = $this->normalizeCode($sourceAccount['Code'] ?? null);
        $destinationCode = $this->normalizeCode($destinationAccount['Code'] ?? null);

        if ($sourceCode && $destinationCode && $sourceCode === $destinationCode) {
            return true;
        }

        $sourceName = $this->normalizeName($sourceAccount['Name'] ?? '');
        $destinationName = $this->normalizeName($destinationAccount['Name'] ?? '');

        if ($sourceName !== '' && $destinationName !== '' && $sourceName === $destinationName) {
            return true;
        }

        return false;
    }

    private function normalizeCode(?string $value): ?string
    {
        if (!$value) {
            return null;
        }

        $normalized = strtolower(trim($value));
        return $normalized !== '' ? $normalized : null;
    }

    private function normalizeName(?string $value): string
    {
        if (!$value) {
            return '';
        }

        return strtolower(preg_replace('/[^a-z0-9]/i', '', $value));
    }

    private function sanitizeAccount(array $account): array
    {
        return collect($account)
            ->except([
                'AccountID',
                'HasAttachments',
                'HasValidationErrors',
                'UpdatedDateUTC',
                'SystemAccount',
                'ReportingCode',
                'ReportingCodeName',
                'ValidationErrors',
                'Class',
                'Status',
                'EnablePaymentsToAccount',
                'ShowInExpenseClaims',
                'BankAccountNumber',
                'CurrencyCode',
                'AddToWatchlist',
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
}