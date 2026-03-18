<?php

namespace App\Services\Xero;

use App\Models\XeroToken;
use Illuminate\Support\Carbon;
use Illuminate\Support\Facades\Http;

class XeroTokenService
{
    public function refresh(XeroToken $token): XeroToken
    {
        $resp = Http::asForm()
            ->withBasicAuth(config('xero.client.id'), config('xero.client.secret'))
            ->post('https://identity.xero.com/connect/token', [
                'grant_type' => 'refresh_token',
                'refresh_token' => $token->refresh_token,
            ]);

        if (!$resp->successful()) {
            throw new \Exception('Token refresh failed: HTTP '.$resp->status().' '.$resp->body());
        }

        $data = $resp->json();

        if (!isset($data['access_token'], $data['refresh_token'])) {
            throw new \Exception('Token refresh response invalid');
        }

        $expiresAt = isset($data['expires_in'])
            ? Carbon::now()->addSeconds((int) $data['expires_in'])
            : null;

        $token->update([
            'access_token'  => $data['access_token'],
            'refresh_token' => $data['refresh_token'],
            'id_token'      => $data['id_token'] ?? $token->id_token,
            'scope'         => $data['scope'] ?? $token->scope,
            'expires_at'    => $expiresAt,
        ]);

        return $token->fresh();
    }

    public function isExpiringSoon(XeroToken $token, int $minutes = 5): bool
    {
        if (!$token->expires_at) {
            return false;
        }

        return $token->expires_at->lte(now()->addMinutes($minutes));
    }
}