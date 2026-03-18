<?php

return [

    'client' => [
        'id' => env('XERO_APP_CLIENT_ID'),
        'secret' => env('XERO_APP_CLIENT_SECRET'),
    ],
    'redirect_uri' => env('XERO_APP_REDIRECT_URI'),
    'connections_url' => env('XERO_APP_CONNECTIONS_URL', 'https://api.xero.com/connections'),
    'scopes' => explode(' ', env('XERO_APP_SCOPES')),

];
