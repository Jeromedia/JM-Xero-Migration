<?php

namespace App\Console\Commands\BrandingThemes;

use App\Models\XeroOrganisation;
use App\Services\Xero\XeroIdMapper;
use Illuminate\Console\Command;
use Illuminate\Http\Client\Response;
use Illuminate\Support\Collection;
use Illuminate\Support\Facades\Http;

class MapBrandingThemesManual extends Command
{
    protected $signature = 'xero:branding-themes:map-manual
                            {--live : Use target instead of test}';

    protected $description = 'Store approved manual branding theme mappings from SOURCE to TEST or TARGET';

    private const ENTITY = 'branding_theme';

    public function handle(): int
    {
        $destinationRole = $this->option('live') ? 'target' : 'test';

        $this->info('Starting branding themes manual mapping...');
        $this->info('Source role: source');
        $this->info('Destination role: ' . $destinationRole);

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

        $this->info('Fetching SOURCE branding themes...');
        $sourceThemes = $this->fetchBrandingThemes($source, 'SOURCE');

        $this->info('Fetching destination branding themes...');
        $destinationThemes = $this->fetchBrandingThemes($destination, strtoupper($destinationRole));

        $sourceByName = $sourceThemes
            ->filter(fn ($theme) => !empty($theme['Name']))
            ->keyBy(fn ($theme) => $this->normalizeName($theme['Name']));

        $destinationByName = $destinationThemes
            ->filter(fn ($theme) => !empty($theme['Name']))
            ->keyBy(fn ($theme) => $this->normalizeName($theme['Name']));

        $manualMap = [
            'Guard + Remote Service Invoice' => 'Guard+RemoteServInv2022',
            'Mon Monthly' => 'MonMonthlyInv2022',
            'Mon Monthly per week' => 'MonMonthlyperWeekInv2022',
            'Mon 13 Weekly (synch)' => 'Mon13WeeklySynchInv2022',
            'Mon 3 Monthly (cycle)' => 'Mon3MonthlyCycleInv2022',
            'Monitoring Service' => 'Monitoring Service Templates',
            'Standard' => 'Standard',
            'Monitoring Service CASTLE' => 'MonServiceInvCASTLE2022',
            'Mon Monthly per week CASTLE' => 'MonMonthlyperWeekInvCASTLE',
            'Version 2 - Guard & Remote' => 'V2Guard+RemoteServInv2022',
            // 'TEST' intentionally ignored
        ];

        $processed = 0;
        $stored = 0;

        foreach ($manualMap as $sourceName => $destinationName) {
            $processed++;

            $sourceTheme = $sourceByName->get($this->normalizeName($sourceName));
            if (!$sourceTheme) {
                $this->error("Source branding theme not found: {$sourceName}");
                return Command::FAILURE;
            }

            $destinationTheme = $destinationByName->get($this->normalizeName($destinationName));
            if (!$destinationTheme) {
                $this->error("Destination branding theme not found: {$destinationName}");
                return Command::FAILURE;
            }

            $sourceId = $sourceTheme['BrandingThemeID'] ?? null;
            $destinationId = $destinationTheme['BrandingThemeID'] ?? null;

            if (!$sourceId || !$destinationId) {
                $this->error("Invalid branding theme IDs for mapping: {$sourceName} -> {$destinationName}");
                return Command::FAILURE;
            }

            $this->storeMapping(
                $mapper,
                $destinationRole,
                $source,
                $destination,
                $sourceId,
                $destinationId,
                $sourceName
            );

            $verifiedId = $destinationRole === 'test'
                ? $mapper->getTestId(self::ENTITY, $sourceId)
                : $mapper->getTargetId(self::ENTITY, $sourceId);

            if (!$verifiedId || $verifiedId !== $destinationId) {
                $this->error("Mapping verification failed: {$sourceName} -> {$destinationName}");
                return Command::FAILURE;
            }

            $stored++;
            $this->info("Stored mapping → {$sourceName} => {$destinationName} ({$destinationId})");
        }

        $this->newLine();
        $this->info('Done.');
        $this->line("Processed: {$processed}");
        $this->line("Stored: {$stored}");
        $this->line('Ignored: TEST');

        return Command::SUCCESS;
    }

    private function fetchBrandingThemes(XeroOrganisation $organisation, string $label): Collection
    {
        $response = $this->getBrandingThemesWithRetry($organisation, $label);

        return collect($response->json('BrandingThemes', []));
    }

    private function getBrandingThemesWithRetry(XeroOrganisation $organisation, string $label): Response
    {
        $maxAttempts = 6;
        $attempt = 1;

        while ($attempt <= $maxAttempts) {
            $response = Http::withToken($organisation->token->access_token)
                ->withHeaders([
                    'Xero-tenant-id' => $organisation->tenant_id,
                    'Accept' => 'application/json',
                ])
                ->get('https://api.xero.com/api.xro/2.0/BrandingThemes');

            if ($response->successful()) {
                return $response;
            }

            if ($response->status() === 429) {
                $retryAfter = (int) ($response->header('Retry-After') ?? 5);
                $retryAfter = $retryAfter > 0 ? $retryAfter : 5;

                $this->warn("Rate limited while fetching {$label} branding themes. Waiting {$retryAfter} seconds before retry {$attempt}/{$maxAttempts}...");
                sleep($retryAfter);
                $attempt++;
                continue;
            }

            $this->error("Failed to fetch {$label} branding themes.");
            $this->line('HTTP Status: ' . $response->status());
            $this->line($response->body());
            throw new \RuntimeException("Failed to fetch {$label} branding themes.");
        }

        throw new \RuntimeException("Failed to fetch {$label} branding themes after retries.");
    }

    private function normalizeName(?string $value): string
    {
        if (!$value) {
            return '';
        }

        return mb_strtolower(trim(preg_replace('/\s+/', ' ', $value)));
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