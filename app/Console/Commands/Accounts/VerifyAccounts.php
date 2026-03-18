<?php

namespace App\Console\Commands\Accounts;

use App\Models\XeroOrganisation;
use Illuminate\Console\Command;
use Illuminate\Support\Facades\Http;

class VerifyAccounts extends Command
{
    protected $signature = 'xero:accounts:verify';
    protected $description = 'Verify that SOURCE and TARGET charts of accounts match';

    public function handle(): int
    {
        $source = XeroOrganisation::where('role', 'source')->first();
        $destination = XeroOrganisation::where('role', 'target')->first();

        if (!$source || !$destination) {
            $this->error('Source or destination organisation not found.');
            return Command::FAILURE;
        }

        $this->info('Fetching SOURCE accounts...');

        $sourceAccounts = collect(
            Http::withToken($source->token->access_token)
                ->withHeaders([
                    'Xero-tenant-id' => $source->tenant_id
                ])
                ->get('https://api.xero.com/api.xro/2.0/Accounts')
                ->json('Accounts')
        );

        $this->info('Fetching DESTINATION accounts...');

        $destinationAccounts = collect(
            Http::withToken($destination->token->access_token)
                ->withHeaders([
                    'Xero-tenant-id' => $destination->tenant_id
                ])
                ->get('https://api.xero.com/api.xro/2.0/Accounts')
                ->json('Accounts')
        );

        $destinationByCode = $destinationAccounts->keyBy('Code');

        $missing = [];
        $typeMismatch = [];
        $nameMismatch = [];
        $taxMismatch = [];

        foreach ($sourceAccounts as $account) {

            $code = $account['Code'];

            if (!$destinationByCode->has($code)) {
                $missing[] = $code . ' - ' . $account['Name'];
                continue;
            }

            $dest = $destinationByCode[$code];

            if ($dest['Type'] !== $account['Type']) {
                $typeMismatch[] = [
                    $code,
                    $account['Name'],
                    $account['Type'],
                    $dest['Type']
                ];
            }

            if ($dest['Name'] !== $account['Name']) {
                $nameMismatch[] = [
                    $code,
                    $account['Name'],
                    $dest['Name']
                ];
            }

            if (($dest['TaxType'] ?? null) !== ($account['TaxType'] ?? null)) {
                $taxMismatch[] = [
                    $code,
                    $account['Name'],
                    $account['TaxType'] ?? '-',
                    $dest['TaxType'] ?? '-'
                ];
            }
        }

        $this->line('');
        $this->info('SOURCE accounts: ' . $sourceAccounts->count());
        $this->info('DESTINATION accounts: ' . $destinationAccounts->count());
        $this->line('');

        if (count($missing) === 0) {
            $this->info('✓ No missing accounts');
        } else {
            $this->error('Missing accounts: ' . count($missing));
            foreach (array_slice($missing, 0, 20) as $item) {
                $this->line($item);
            }
        }

        if (count($typeMismatch) > 0) {
            $this->error('Type mismatches detected:');
            $this->table(
                ['Code', 'Name', 'Source Type', 'Destination Type'],
                $typeMismatch
            );
        }

        if (count($nameMismatch) > 0) {
            $this->error('Name mismatches detected:');
            $this->table(
                ['Code', 'Source Name', 'Destination Name'],
                $nameMismatch
            );
        }

        if (count($taxMismatch) > 0) {
            $this->error('TaxType mismatches detected:');
            $this->table(
                ['Code', 'Name', 'Source TaxType', 'Destination TaxType'],
                $taxMismatch
            );
        }

        if (
            count($missing) === 0 &&
            count($typeMismatch) === 0 &&
            count($nameMismatch) === 0 &&
            count($taxMismatch) === 0
        ) {
            $this->info('✓ Charts of accounts match perfectly');
        }

        return Command::SUCCESS;
    }
}