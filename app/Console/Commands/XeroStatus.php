<?php

namespace App\Console\Commands;

use App\Models\XeroOrganisation;
use App\Models\XeroToken;
use App\Services\Xero\XeroTokenService;
use Illuminate\Console\Command;

class XeroStatus extends Command
{
    protected $signature = 'xero:status';
    protected $description = 'Show latest Xero token + organisations';

    public function handle(XeroTokenService $service): int
    {
        $token = XeroToken::latest()->first();

        if (!$token) {
            $this->error('No token found. Connect first via /xero/connect');
            return self::FAILURE;
        }

        $this->info('=== Xero Status ===');

        $expiresAt = $token->expires_at;

        $expiresIn = $expiresAt
            ? ($expiresAt->isPast()
                ? 'expired '.$expiresAt->diffForHumans()
                : $expiresAt->diffForHumans())
            : '(null)';

        $this->line('');
        $this->info('Token expires in: '.$expiresIn);
        $this->line('');

        if ($this->confirm('Refresh token now?', false)) {

            try {

                $this->info('Refreshing token...');
                $token = $service->refresh($token);

                $this->info('✓ Token refreshed');

            } catch (\Throwable $e) {

                $this->error($e->getMessage());

            }

            $this->line('');
        }

        $this->displayStatus($token);

        return self::SUCCESS;
    }

    private function displayStatus($token)
    {
        $expiresAt = $token->expires_at;

        $expiresIn = $expiresAt
            ? ($expiresAt->isPast()
                ? 'expired '.$expiresAt->diffForHumans()
                : $expiresAt->diffForHumans())
            : '(null)';

        $this->table(
            ['token_id','expires_at','expires_in','has_access','has_refresh'],
            [[
                $token->id,
                $expiresAt?->toDateTimeString(),
                $expiresIn,
                $token->access_token ? 'yes':'no',
                $token->refresh_token ? 'yes':'no',
            ]]
        );

        $this->line('');
        $this->info('Access Token:');
        $this->line($token->access_token ?? '(null)');

        $this->line('');
        $this->info('Refresh Token:');
        $this->line($token->refresh_token ?? '(null)');
        $this->line('');

        $orgs = XeroOrganisation::orderBy('role')->orderBy('tenant_name')->get();

        if ($orgs->isEmpty()) {
            $this->warn('No organisations saved yet.');
            return;
        }

        $rows = $orgs->map(fn($o)=>[
            strtoupper($o->role),
            $o->tenant_name,
            $o->tenant_type,
            $o->tenant_id,
            $o->xero_token_id
        ])->toArray();

        $this->table(['Role','tenantName','tenantType','tenantId','token_id'],$rows);
    }
}