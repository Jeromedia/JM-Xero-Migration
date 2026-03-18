<?php

namespace App\Console\Commands;

use App\Models\XeroToken;
use App\Services\Xero\XeroTokenService;
use Illuminate\Console\Command;

class XeroTokenRefresh extends Command
{
    protected $signature = 'xero:token-refresh {--token=}';
    protected $description = 'Refresh Xero OAuth token';

    public function handle(XeroTokenService $service): int
    {
        $tokenId = $this->option('token');

        $token = $tokenId
            ? XeroToken::find($tokenId)
            : XeroToken::latest()->first();

        if (!$token) {
            $this->error('No token found.');
            return self::FAILURE;
        }

        $this->info('Refreshing token for token_id: '.$token->id);

        try {

            $token = $service->refresh($token);

            $this->info('✓ Token refreshed');
            $this->line('Expires at: '.$token->expires_at);

        } catch (\Throwable $e) {

            $this->error($e->getMessage());
            return self::FAILURE;

        }

        return self::SUCCESS;
    }
}