<?php

namespace App\Console\Commands\Accounts;

use App\Models\XeroOrganisation;
use App\Services\Xero\XeroIdMapper;
use Illuminate\Console\Command;
use Illuminate\Http\Client\Response;
use Illuminate\Support\Collection;
use Illuminate\Support\Facades\Http;
use Illuminate\Support\Facades\Log;

class SyncAccounts extends Command
{
    protected $signature = 'xero:accounts:sync
                            {--live : Use target instead of test}
                            {--code= : Sync only one account by exact Code}
                            {--source-id= : Sync only one account by source AccountID}
                            {--first : Sync only the first source account}';

    protected $description = 'Sync chart of accounts from SOURCE to TEST or TARGET with mapping validation and recovery';

    private const ENTITY = 'account';

    public function handle(): int
    {
        $destinationRole = $this->option('live') ? 'target' : 'test';

        $this->info('Starting accounts sync (VALIDATED SMART MODE)...');
        $this->info('Source role: source');
        $this->info('Destination role: ' . $destinationRole);

        if ($this->option('code')) {
            $this->info('Filter mode: exact code = ' . $this->option('code'));
        }

        if ($this->option('source-id')) {
            $this->info('Filter mode: source AccountID = ' . $this->option('source-id'));
        }

        if ($this->option('first')) {
            $this->info('Filter mode: first source account only');
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

        $this->info('Loading destination accounts index...');
        $destinationAccounts = $this->fetchAccounts($destination, strtoupper($destinationRole));
        [$destinationById, $destinationByCode] = $this->buildDestinationIndexes($destinationAccounts);

        $this->info('Fetching SOURCE accounts...');
        $sourceAccounts = $this->fetchAccounts($source, 'SOURCE');
        $sourceAccounts = $this->filterSourceAccounts($sourceAccounts);

        if ($sourceAccounts->isEmpty()) {
            $this->info('No accounts found.');
            return Command::SUCCESS;
        }

        if ($this->option('first')) {
            $sourceAccounts = collect([$sourceAccounts->first()]);
        }

        $totalProcessed = 0;
        $totalMappedValid = 0;
        $totalMappedInvalid = 0;
        $totalRecovered = 0;
        $totalCreated = 0;
        $totalConflicts = 0;

        foreach ($sourceAccounts as $sourceAccount) {
            $totalProcessed++;

            $sourceId = $sourceAccount['AccountID'] ?? null;
            $code = $sourceAccount['Code'] ?? null;
            $name = $sourceAccount['Name'] ?? null;

            if (!$sourceId || !$code || !$name) {
                $this->error('Invalid source account.');
                $this->line(json_encode($sourceAccount, JSON_PRETTY_PRINT | JSON_UNESCAPED_SLASHES));
                return Command::FAILURE;
            }

            $mappedId = $destinationRole === 'test'
                ? $mapper->getTestId(self::ENTITY, $sourceId)
                : $mapper->getTargetId(self::ENTITY, $sourceId);

            if ($mappedId) {
                $mappedAccount = $destinationById->get($mappedId);

                if ($mappedAccount && $this->isReasonableMappedMatch($sourceAccount, $mappedAccount)) {
                    $totalMappedValid++;
                    $this->info("Mapped already → Valid: {$code} - {$name} ({$mappedId})");
                    continue;
                }

                $totalMappedInvalid++;
            }

            $destinationCodeMatch = $this->findDestinationCodeMatch($sourceAccount, $destinationByCode);

            if ($destinationCodeMatch) {
                if ($this->isReasonableMappedMatch($sourceAccount, $destinationCodeMatch)) {
                    $this->storeMapping(
                        $mapper,
                        $destinationRole,
                        $source,
                        $destination,
                        $sourceId,
                        $destinationCodeMatch['AccountID'],
                        $name
                    );

                    $verifiedId = $destinationRole === 'test'
                        ? $mapper->getTestId(self::ENTITY, $sourceId)
                        : $mapper->getTargetId(self::ENTITY, $sourceId);

                    if (!$verifiedId || $verifiedId !== $destinationCodeMatch['AccountID']) {
                        $this->error("Mapping verification failed for recovered account: {$code} - {$name}");
                        return Command::FAILURE;
                    }

                    $totalRecovered++;
                    $this->info("Recovered + mapped: {$code} - {$name} ({$destinationCodeMatch['AccountID']})");
                    continue;
                }

                $totalConflicts++;
                $this->error("Account conflict detected for code {$code}");
                $this->line('SOURCE: ' . ($sourceAccount['Name'] ?? '') . ' | ' . ($sourceAccount['Type'] ?? '') . ' | ' . ($sourceAccount['Status'] ?? ''));
                $this->line('TARGET: ' . ($destinationCodeMatch['Name'] ?? '') . ' | ' . ($destinationCodeMatch['Type'] ?? '') . ' | ' . ($destinationCodeMatch['Status'] ?? ''));

                Log::error('Xero account sync conflict', [
                    'source_account_id' => $sourceId,
                    'code' => $code,
                    'source_name' => $sourceAccount['Name'] ?? null,
                    'source_type' => $sourceAccount['Type'] ?? null,
                    'source_status' => $sourceAccount['Status'] ?? null,
                    'target_account_id' => $destinationCodeMatch['AccountID'] ?? null,
                    'target_name' => $destinationCodeMatch['Name'] ?? null,
                    'target_type' => $destinationCodeMatch['Type'] ?? null,
                    'target_status' => $destinationCodeMatch['Status'] ?? null,
                ]);

                return Command::FAILURE;
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

            $put = $this->putAccountsWithRetry($destination, $payload, $code, $name);
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
                    [$destinationById, $destinationByCode] = $this->buildDestinationIndexes($destinationAccounts);
                }

                $totalCreated++;
                $this->info("Created + mapped: {$code} - {$name} ({$destinationId})");
                continue;
            }

            $this->warn("Create not completed -> refreshing destination index and retrying recovery: {$code} - {$name}");

            $destinationAccounts = $this->fetchAccounts($destination, strtoupper($destinationRole));
            [$destinationById, $destinationByCode] = $this->buildDestinationIndexes($destinationAccounts);

            $destinationCodeMatch = $this->findDestinationCodeMatch($sourceAccount, $destinationByCode);

            if ($destinationCodeMatch && $this->isReasonableMappedMatch($sourceAccount, $destinationCodeMatch)) {
                $this->storeMapping(
                    $mapper,
                    $destinationRole,
                    $source,
                    $destination,
                    $sourceId,
                    $destinationCodeMatch['AccountID'],
                    $name
                );

                $verifiedId = $destinationRole === 'test'
                    ? $mapper->getTestId(self::ENTITY, $sourceId)
                    : $mapper->getTargetId(self::ENTITY, $sourceId);

                if (!$verifiedId || $verifiedId !== $destinationCodeMatch['AccountID']) {
                    $this->error("Mapping verification failed after recovery for: {$code} - {$name}");
                    return Command::FAILURE;
                }

                $totalRecovered++;
                $this->info("Recovered after refresh + mapped: {$code} - {$name} ({$destinationCodeMatch['AccountID']})");
                continue;
            }

            $this->error("Unable to create or recover account: {$code} - {$name}");
            $this->line('HTTP Status: ' . $put->status());
            $this->line($put->body());
            $this->line(json_encode($payload, JSON_PRETTY_PRINT | JSON_UNESCAPED_SLASHES));

            Log::error('Xero account migration failed', [
                'source_account_id' => $sourceId,
                'code' => $code,
                'name' => $name,
                'payload' => $payload,
                'status' => $put->status(),
                'response' => $put->body(),
            ]);

            return Command::FAILURE;
        }

        $this->newLine();
        $this->info('Done.');
        $this->line("Total processed: {$totalProcessed}");
        $this->line("Valid mapped: {$totalMappedValid}");
        $this->line("Invalid mapped found: {$totalMappedInvalid}");
        $this->line("Recovered + mapped: {$totalRecovered}");
        $this->line("Created + mapped: {$totalCreated}");
        $this->line("Conflicts found: {$totalConflicts}");

        return Command::SUCCESS;
    }

    private function fetchAccounts(XeroOrganisation $organisation, string $label): Collection
    {
        $response = $this->getAccountsWithRetry($organisation, $label);

        return collect($response->json('Accounts', []));
    }

    private function getAccountsWithRetry(XeroOrganisation $organisation, string $label): Response
    {
        $maxAttempts = 6;
        $attempt = 1;

        while ($attempt <= $maxAttempts) {
            $response = Http::withToken($organisation->token->access_token)
                ->withHeaders([
                    'Xero-tenant-id' => $organisation->tenant_id,
                    'Accept' => 'application/json',
                ])
                ->get('https://api.xero.com/api.xro/2.0/Accounts');

            if ($response->successful()) {
                return $response;
            }

            if ($response->status() === 429) {
                $retryAfter = (int) ($response->header('Retry-After') ?? 5);
                $retryAfter = $retryAfter > 0 ? $retryAfter : 5;

                $this->warn("Rate limited while fetching {$label} accounts. Waiting {$retryAfter} seconds before retry {$attempt}/{$maxAttempts}...");
                sleep($retryAfter);
                $attempt++;
                continue;
            }

            $this->error("Failed to fetch {$label} accounts.");
            $this->line('HTTP Status: ' . $response->status());
            $this->line($response->body());
            throw new \RuntimeException("Failed to fetch {$label} accounts.");
        }

        $this->error("Failed to fetch {$label} accounts after retries.");
        throw new \RuntimeException("Failed to fetch {$label} accounts after retries.");
    }

    private function putAccountsWithRetry(
        XeroOrganisation $organisation,
        array $payload,
        string $code,
        string $name
    ): Response {
        $maxAttempts = 6;
        $attempt = 1;

        while ($attempt <= $maxAttempts) {
            $response = Http::withToken($organisation->token->access_token)
                ->withHeaders([
                    'Xero-tenant-id' => $organisation->tenant_id,
                    'Accept' => 'application/json',
                    'Content-Type' => 'application/json',
                ])
                ->put('https://api.xero.com/api.xro/2.0/Accounts', $payload);

            if ($response->successful()) {
                return $response;
            }

            if ($response->status() === 429) {
                $retryAfter = (int) ($response->header('Retry-After') ?? 5);
                $retryAfter = $retryAfter > 0 ? $retryAfter : 5;

                $this->warn("Rate limited while creating account {$code} - {$name}. Waiting {$retryAfter} seconds before retry {$attempt}/{$maxAttempts}...");
                sleep($retryAfter);
                $attempt++;
                continue;
            }

            return $response;
        }

        $this->error("Rate limited while creating account {$code} - {$name} after retries.");
        throw new \RuntimeException("Rate limited while creating account {$code} - {$name} after retries.");
    }

    private function filterSourceAccounts(Collection $accounts): Collection
    {
        if ($this->option('source-id')) {
            $sourceId = trim((string) $this->option('source-id'));

            return $accounts
                ->filter(fn(array $account) => ($account['AccountID'] ?? '') === $sourceId)
                ->values();
        }

        if ($this->option('code')) {
            $code = trim((string) $this->option('code'));

            return $accounts
                ->filter(fn(array $account) => trim((string) ($account['Code'] ?? '')) === $code)
                ->values();
        }

        return $accounts->values();
    }

    private function buildDestinationIndexes(Collection $accounts): array
    {
        $byId = $accounts
            ->filter(fn($account) => !empty($account['AccountID']))
            ->keyBy('AccountID');

        $byCode = $accounts
            ->filter(fn($account) => !empty($account['Code']))
            ->groupBy(fn($account) => $this->normalizeCode($account['Code']));

        return [$byId, $byCode];
    }

    private function findDestinationCodeMatch(array $sourceAccount, Collection $destinationByCode): ?array
    {
        $codeKey = $this->normalizeCode($sourceAccount['Code'] ?? null);

        if (!$codeKey) {
            return null;
        }

        $matches = $destinationByCode->get($codeKey);

        if ($matches && $matches->count() === 1) {
            return $matches->first();
        }

        return null;
    }

    private function isReasonableMappedMatch(array $sourceAccount, array $destinationAccount): bool
    {
        $sourceCode = $this->normalizeCode($sourceAccount['Code'] ?? null);
        $destinationCode = $this->normalizeCode($destinationAccount['Code'] ?? null);

        $sourceName = $this->normalizeName($sourceAccount['Name'] ?? '');
        $destinationName = $this->normalizeName($destinationAccount['Name'] ?? '');

        $sourceType = strtoupper(trim((string) ($sourceAccount['Type'] ?? '')));
        $destinationType = strtoupper(trim((string) ($destinationAccount['Type'] ?? '')));

        return $sourceCode
            && $destinationCode
            && $sourceCode === $destinationCode
            && $sourceName !== ''
            && $destinationName !== ''
            && $sourceName === $destinationName
            && $sourceType !== ''
            && $destinationType !== ''
            && $sourceType === $destinationType;
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
        $clean = collect($account)
            ->except([
                'AccountID',
                'HasAttachments',
                'HasValidationErrors',
                'UpdatedDateUTC',
                'SystemAccount',
                'ReportingCode',
                'ReportingCodeName',
                'ReportingCodeUpdatedUTC',
                'ValidationErrors',
                'Class',
                'Status',
                'EnablePaymentsToAccount',
                'ShowInExpenseClaims',
                'CurrencyCode',
                'AddToWatchlist',
            ])
            ->map(fn($value) => $this->sanitizeValue($value))
            ->filter(function ($value) {
                if (is_array($value)) {
                    return !empty($value);
                }

                return $value !== null && $value !== '';
            })
            ->toArray();

        if (($clean['Type'] ?? null) === 'BANK') {
            unset($clean['TaxType']);
        }

        return $clean;
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
