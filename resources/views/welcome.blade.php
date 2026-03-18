<!DOCTYPE html>
<html lang="{{ str_replace('_', '-', app()->getLocale()) }}">

<head>
    <meta charset="utf-8">
    <meta name="viewport" content="width=device-width, initial-scale=1">

    <title>{{ config('app.name', 'Laravel') }}</title>

    <!-- Fonts -->
    <link rel="preconnect" href="https://fonts.bunny.net">
    <link href="https://fonts.bunny.net/css?family=instrument-sans:400,500,600" rel="stylesheet" />

    <!-- Styles / Scripts -->
    @if (file_exists(public_path('build/manifest.json')) || file_exists(public_path('hot')))
        @vite(['resources/css/app.css', 'resources/js/app.js'])
    @else
        <style>
            body {
                font-size: 1rem;
            }
        </style>
    @endif
</head>

<body style="font-family: sans-serif; padding: 40px;">
    <h1>Xero Migration Utility</h1>

    <a href="{{ route('xero.connect') }}"
        style="display:inline-block;padding:12px 18px;background:#0ea5e9;color:white;text-decoration:none;border-radius:8px;">
        Connect to Xero (Get Started)
    </a>
</body>

</html>
