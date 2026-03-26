<?php

namespace App\Console\Commands\Accounts;

use App\Models\XeroOrganisation;
use Illuminate\Console\Command;
use Illuminate\Http\Client\Response;
use Illuminate\Support\Collection;
use Illuminate\Support\Facades\Http;

class CompareAccounts extends Command
{
    protected $signature = 'xero:accounts:compare {--live : Compare SOURCE against TARGET instead of TEST}';
    protected $description = 'Compare chart of accounts between SOURCE and TEST or TARGET';

    public function handle(): int
    {
        $destinationRole = $this->option('live') ? 'target' : 'test';

        $this->info('Starting accounts comparison...');
        $this->info('Source role: source');
        $this->info('Destination role: ' . $destinationRole);

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

        $this->info('Fetching SOURCE accounts...');
        $sourceAccounts = $this->fetchAccounts($source, 'SOURCE');

        $this->info('Fetching ' . strtoupper($destinationRole) . ' accounts...');
        $destinationAccounts = $this->fetchAccounts($destination, strtoupper($destinationRole));

        $sourceByCode = $this->indexByCode($sourceAccounts);
        $destinationByCode = $this->indexByCode($destinationAccounts);

        $matching = collect();
        $missingInDestination = collect();
        $extraInDestination = collect();
        $nameMismatches = collect();
        $typeMismatches = collect();
        $statusDifferences = collect();

        foreach ($sourceByCode as $code => $sourceAccount) {
            $destinationAccount = $destinationByCode->get($code);

            if (!$destinationAccount) {
                $missingInDestination->push($sourceAccount);
                continue;
            }

            $matching->push([
                'Code' => $sourceAccount['Code'] ?? '',
                'SourceName' => $sourceAccount['Name'] ?? '',
                'DestinationName' => $destinationAccount['Name'] ?? '',
                'SourceType' => $sourceAccount['Type'] ?? '',
                'DestinationType' => $destinationAccount['Type'] ?? '',
                'SourceStatus' => $sourceAccount['Status'] ?? '',
                'DestinationStatus' => $destinationAccount['Status'] ?? '',
            ]);

            if (($sourceAccount['Name'] ?? '') !== ($destinationAccount['Name'] ?? '')) {
                $nameMismatches->push([
                    'Code' => $sourceAccount['Code'] ?? '',
                    'SourceName' => $sourceAccount['Name'] ?? '',
                    'DestinationName' => $destinationAccount['Name'] ?? '',
                ]);
            }

            if (($sourceAccount['Type'] ?? '') !== ($destinationAccount['Type'] ?? '')) {
                $typeMismatches->push([
                    'Code' => $sourceAccount['Code'] ?? '',
                    'Name' => $sourceAccount['Name'] ?? '',
                    'SourceType' => $sourceAccount['Type'] ?? '',
                    'DestinationType' => $destinationAccount['Type'] ?? '',
                ]);
            }

            if (($sourceAccount['Status'] ?? '') !== ($destinationAccount['Status'] ?? '')) {
                $statusDifferences->push([
                    'Code' => $sourceAccount['Code'] ?? '',
                    'Name' => $sourceAccount['Name'] ?? '',
                    'SourceStatus' => $sourceAccount['Status'] ?? '',
                    'DestinationStatus' => $destinationAccount['Status'] ?? '',
                ]);
            }
        }

        foreach ($destinationByCode as $code => $destinationAccount) {
            if (!$sourceByCode->has($code)) {
                $extraInDestination->push($destinationAccount);
            }
        }

        $this->newLine();
        $this->info('Summary');
        $this->line('SOURCE accounts: ' . $sourceAccounts->count());
        $this->line(strtoupper($destinationRole) . ' accounts: ' . $destinationAccounts->count());

        $this->newLine();
        $this->info('Comparison');
        $this->line('Matching by Code: ' . $matching->count());
        $this->line('Missing in ' . strtoupper($destinationRole) . ': ' . $missingInDestination->count());
        $this->line('Extra in ' . strtoupper($destinationRole) . ': ' . $extraInDestination->count());
        $this->line('Name mismatches: ' . $nameMismatches->count());
        $this->line('Type mismatches: ' . $typeMismatches->count());
        $this->line('Status differences: ' . $statusDifferences->count());

        if ($missingInDestination->isNotEmpty()) {
            $this->newLine();
            $this->warn('Missing in ' . strtoupper($destinationRole) . ':');
            foreach ($missingInDestination as $account) {
                $this->line(sprintf(
                    ' - %s | %s | %s | %s',
                    $account['Code'] ?? '',
                    $account['Name'] ?? '',
                    $account['Type'] ?? '',
                    $account['Status'] ?? ''
                ));
            }
        }

        if ($extraInDestination->isNotEmpty()) {
            $this->newLine();
            $this->warn('Extra in ' . strtoupper($destinationRole) . ':');
            foreach ($extraInDestination as $account) {
                $this->line(sprintf(
                    ' - %s | %s | %s | %s',
                    $account['Code'] ?? '',
                    $account['Name'] ?? '',
                    $account['Type'] ?? '',
                    $account['Status'] ?? ''
                ));
            }
        }

        if ($nameMismatches->isNotEmpty()) {
            $this->newLine();
            $this->warn('Name mismatches:');
            foreach ($nameMismatches as $row) {
                $this->line(sprintf(
                    ' - %s | SOURCE: %s | DESTINATION: %s',
                    $row['Code'],
                    $row['SourceName'],
                    $row['DestinationName']
                ));
            }
        }

        if ($typeMismatches->isNotEmpty()) {
            $this->newLine();
            $this->warn('Type mismatches:');
            foreach ($typeMismatches as $row) {
                $this->line(sprintf(
                    ' - %s | %s | SOURCE: %s | DESTINATION: %s',
                    $row['Code'],
                    $row['Name'],
                    $row['SourceType'],
                    $row['DestinationType']
                ));
            }
        }

        if ($statusDifferences->isNotEmpty()) {
            $this->newLine();
            $this->warn('Status differences:');
            foreach ($statusDifferences as $row) {
                $this->line(sprintf(
                    ' - %s | %s | SOURCE: %s | DESTINATION: %s',
                    $row['Code'],
                    $row['Name'],
                    $row['SourceStatus'],
                    $row['DestinationStatus']
                ));
            }
        }

        $this->newLine();
        $this->info('Comparison complete.');

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

    private function indexByCode(Collection $accounts): Collection
    {
        return $accounts
            ->filter(fn ($account) => !empty($account['Code']))
            ->keyBy(fn ($account) => $this->normalizeCode($account['Code']));
    }

    private function normalizeCode(?string $value): ?string
    {
        if (!$value) {
            return null;
        }

        $normalized = strtolower(trim($value));

        return $normalized !== '' ? $normalized : null;
    }
}