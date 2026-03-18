<?php

namespace App\Console\Commands\Accounts;

use App\Models\XeroOrganisation;
use App\Services\Xero\XeroIdMapper;
use Illuminate\Console\Command;
use Illuminate\Support\Facades\Http;

class SyncAccounts extends Command
{
    protected $signature = 'xero:accounts:sync {--live : Use target instead of test}';
    protected $description = 'Sync chart of accounts from SOURCE to TEST or TARGET';

    public function handle(): int
    {
        $destinationRole = $this->option('live') ? 'target' : 'test';

        $this->info('Starting accounts synchronization...');
        $this->info('Source role: source');
        $this->info('Destination role: ' . $destinationRole);

        $mapper = new XeroIdMapper();

        $source = XeroOrganisation::where('role', 'source')->first();
        $destination = XeroOrganisation::where('role', $destinationRole)->first();

        if (!$source || !$destination) {
            $this->error('Source or destination organisation not found.');
            return Command::FAILURE;
        }

        $this->info('Fetching SOURCE accounts...');

        $sourceResponse = Http::withToken($source->token->access_token)
            ->withHeaders([
                'Xero-tenant-id' => $source->tenant_id,
                'Accept' => 'application/json',
            ])
            ->get('https://api.xero.com/api.xro/2.0/Accounts');

        if (!$sourceResponse->successful()) {
            $this->error('Failed to fetch SOURCE accounts.');
            $this->line($sourceResponse->body());
            return Command::FAILURE;
        }

        $sourceAccounts = collect($sourceResponse->json('Accounts', []));

        $this->info('Fetching DESTINATION accounts...');

        $destinationResponse = Http::withToken($destination->token->access_token)
            ->withHeaders([
                'Xero-tenant-id' => $destination->tenant_id,
                'Accept' => 'application/json',
            ])
            ->get('https://api.xero.com/api.xro/2.0/Accounts');

        if (!$destinationResponse->successful()) {
            $this->error('Failed to fetch DESTINATION accounts.');
            $this->line($destinationResponse->body());
            return Command::FAILURE;
        }

        $destinationAccounts = collect($destinationResponse->json('Accounts', []));
        $destinationByCode = $destinationAccounts->keyBy('Code');

        $alreadyMapped = 0;
        $accountsToProcess = collect();

        foreach ($sourceAccounts as $account) {
            $sourceAccountId = $account['AccountID'] ?? null;

            if (!$sourceAccountId) {
                continue;
            }

            $mapped = $destinationRole === 'test'
                ? $mapper->getTestId('account', $sourceAccountId)
                : $mapper->getTargetId('account', $sourceAccountId);

            if ($mapped) {
                $alreadyMapped++;
                continue;
            }

            $accountsToProcess->push($account);
        }

        $this->line('');
        $this->info('SOURCE accounts: ' . $sourceAccounts->count());
        $this->info('DESTINATION accounts: ' . $destinationAccounts->count());
        $this->info('Already migrated (mapper): ' . $alreadyMapped);
        $this->info('Accounts to process: ' . $accountsToProcess->count());
        $this->info('Ready to process: ' . $accountsToProcess->count());

        $this->line('');
        $this->info('Preview (first 10 accounts):');

        $accountsToProcess->take(10)->each(function ($account) {
            $this->line(str_pad($account['Code'] ?? '', 6) . ' ' . ($account['Name'] ?? ''));
        });

        $this->line('');

        $mode = $this->choice(
            'Select migration mode',
            [
                'Test (1 account only)',
                'Batch (all accounts)',
            ],
            0
        );

        if ($mode === 'Test (1 account only)') {
            $accountsToProcess = $accountsToProcess->take(1);
        }

        if ($accountsToProcess->isEmpty()) {
            $this->info('Nothing to migrate.');
            return Command::SUCCESS;
        }

        if (!$this->confirm('Start processing these accounts?', true)) {
            return Command::SUCCESS;
        }

        $progress = $this->output->createProgressBar($accountsToProcess->count());
        $progress->start();

        foreach ($accountsToProcess as $account) {
            if (!empty($account['SystemAccount'])) {
                $progress->advance();
                continue;
            }

            $code = $account['Code'] ?? null;
            $sourceAccountId = $account['AccountID'] ?? null;

            if (!$code || !$sourceAccountId) {
                $progress->advance();
                continue;
            }

            if ($destinationByCode->has($code)) {
                $destAccount = $destinationByCode[$code];

                if (($destAccount['Status'] ?? null) === 'ARCHIVED') {
                    $payload = [
                        'AccountID' => $destAccount['AccountID'],
                        'Status' => 'ACTIVE',
                    ];

                    $response = Http::withToken($destination->token->access_token)
                        ->withHeaders([
                            'Xero-tenant-id' => $destination->tenant_id,
                            'Accept' => 'application/json',
                            'Content-Type' => 'application/json',
                        ])
                        ->post(
                            'https://api.xero.com/api.xro/2.0/Accounts/' . $destAccount['AccountID'],
                            $payload
                        );

                    if (!$response->successful()) {
                        $progress->finish();
                        $this->error('');
                        $this->error('Failed to unarchive account.');
                        $this->error($code . ' - ' . ($account['Name'] ?? ''));
                        $this->line(json_encode($payload, JSON_PRETTY_PRINT));
                        $this->line($response->body());

                        return Command::FAILURE;
                    }

                    $destinationId = $destAccount['AccountID'];
                } else {
                    $destinationId = $destAccount['AccountID'];
                }
            } else {
                $cleanAccount = collect($account)
                    ->except([
                        'AccountID',
                        'HasAttachments',
                        'HasValidationErrors',
                        'UpdatedDateUTC',
                        'Class',
                        'ReportingCode',
                        'ReportingCodeName',
                        'ReportingCodeUpdatedUTC',
                        'Status',
                        'SystemAccount',
                    ])
                    ->filter(function ($value) {
                        return $value !== null && $value !== '';
                    })
                    ->toArray();

                $payload = $cleanAccount;

                $response = Http::withToken($destination->token->access_token)
                    ->withHeaders([
                        'Xero-tenant-id' => $destination->tenant_id,
                        'Accept' => 'application/json',
                        'Content-Type' => 'application/json',
                    ])
                    ->put(
                        'https://api.xero.com/api.xro/2.0/Accounts',
                        $payload
                    );

                if (!$response->successful()) {
                    $progress->finish();
                    $this->error('');
                    $this->error('Account creation FAILED.');
                    $this->error($code . ' - ' . ($account['Name'] ?? ''));
                    $this->line(json_encode($payload, JSON_PRETTY_PRINT));
                    $this->line($response->body());

                    return Command::FAILURE;
                }

                $destinationId = $response->json('Accounts.0.AccountID')
                    ?? $response->json('AccountID');

                if (!$destinationId) {
                    $progress->finish();
                    $this->error('');
                    $this->error('Account creation returned no destination AccountID.');
                    $this->error($code . ' - ' . ($account['Name'] ?? ''));
                    $this->line(json_encode($payload, JSON_PRETTY_PRINT));
                    $this->line($response->body());

                    return Command::FAILURE;
                }
            }

            if ($destinationRole === 'test') {
                $mapper->storeTest(
                    'account',
                    $sourceAccountId,
                    $destinationId,
                    $source->tenant_id,
                    $destination->tenant_id,
                    $account['Name'] ?? null
                );
            } else {
                $mapper->storeTarget(
                    'account',
                    $sourceAccountId,
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
        $this->info('Accounts synchronization completed.');

        return Command::SUCCESS;
    }
}