<?php

namespace App\Http\Controllers;

use App\Models\XeroOrganisation;
use App\Models\XeroToken;
use Illuminate\Http\Request;
use Illuminate\Support\Carbon;
use Illuminate\Support\Facades\Http;

class XeroAuthController extends Controller
{
    // One button "Connect"
    public function connect()
    {
        $query = http_build_query([
            'response_type' => 'code',
            'client_id'     => config('xero.client.id'),
            'redirect_uri'  => config('xero.redirect_uri'),
            'scope'         => implode(' ', config('xero.scopes')),
            'state'         => config('xero.state', 'jeromedia'), // optional
        ]);

        return redirect('https://login.xero.com/identity/connect/authorize?' . $query);
    }

    public function callback(Request $request)
    {
        $code = $request->query('code');

        if (!$code) {
            return response()->json(['error' => 'Missing code'], 400);
        }

        // Exchange code -> token
        $tokenResponse = Http::asForm()
            ->withBasicAuth(config('xero.client.id'), config('xero.client.secret'))
            ->post('https://identity.xero.com/connect/token', [
                'grant_type'   => 'authorization_code',
                'code'         => $code,
                'redirect_uri' => config('xero.redirect_uri'),
            ]);

        $data = $tokenResponse->json();

        if (!isset($data['access_token'], $data['refresh_token'])) {
            return response()->json([
                'error' => 'Token exchange failed',
                'data'  => $data,
            ], 500);
        }

        $expiresAt = isset($data['expires_in'])
            ? Carbon::now()->addSeconds((int) $data['expires_in'])
            : null;

        // Save token (new row each connect is fine on localhost)
        $token = XeroToken::create([
            'access_token'  => $data['access_token'],
            'refresh_token' => $data['refresh_token'],
            'id_token'      => $data['id_token'] ?? null,
            'scope'         => $data['scope'] ?? null,
            'expires_at'    => $expiresAt,
        ]);

        // Fetch connections and store organisations linked to this token
        $connectionsResponse = Http::withHeaders([
            'Authorization' => 'Bearer ' . $data['access_token'],
            'Accept' => 'application/json',
        ])->get(config('xero.connections_url'));

        $connections = $connectionsResponse->json() ?? [];

        foreach ($connections as $c) {
            $tenantId = $c['tenantId'] ?? null;
            if (!$tenantId) continue;

            XeroOrganisation::updateOrCreate(
                [
                    'xero_token_id' => $token->id,
                    'tenant_id' => $tenantId,
                ],
                [
                    'tenant_name' => $c['tenantName'] ?? null,
                    'tenant_type' => $c['tenantType'] ?? null,
                ]
            );
        }

        return response()->json([
            'status' => 'stored',
            'token_id' => $token->id,
            'organisations_saved' => count($connections),
        ]);
    }
}