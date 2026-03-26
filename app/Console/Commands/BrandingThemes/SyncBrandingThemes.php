<?php

namespace App\Console\Commands\BrandingThemes;

use App\Models\XeroOrganisation;
use App\Services\Xero\XeroIdMapper;
use Illuminate\Console\Command;
use Illuminate\Http\Client\Response;
use Illuminate\Support\Collection;
use Illuminate\Support\Facades\Http;
use Illuminate\Support\Facades\Log;

class SyncBrandingThemes extends Command
{
    protected $signature = 'xero:branding-themes:sync
                            {--live : Use target instead of test}
                            {--name= : Sync only one branding theme by exact Name}
                            {--source-id= : Sync only one branding theme by source BrandingThemeID}
                            {--first : Sync only the first source branding theme}';

    protected $description = 'Map branding themes from SOURCE to TEST or TARGET by validated name matching only';

    private const ENTITY = 'branding_theme';

    public function handle(): int
    {
        $destinationRole = $this->option('live') ? 'target' : 'test';

        $this->info('Starting branding themes sync (VALIDATED SMART MODE)...');
        $this->info('Source role: source');
        $this->info('Destination role: ' . $destinationRole);

        if ($this->option('name')) {
            $this->info('Filter mode: exact name = ' . $this->option('name'));
        }

        if ($this->option('source-id')) {
            $this->info('Filter mode: source BrandingThemeID = ' . $this->option('source-id'));
        }

        if ($this->option('first')) {
            $this->info('Filter mode: first source branding theme only');
        }

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

        $this->info('Loading destination branding themes index...');
        $destinationThemes = $this->fetchBrandingThemes($destination, strtoupper($destinationRole));
        [$destinationById, $destinationByName] = $this->buildDestinationIndexes($destinationThemes);

        $this->info('Fetching SOURCE branding themes...');
        $sourceThemes = $this->fetchBrandingThemes($source, 'SOURCE');
        $sourceThemes = $this->filterSourceThemes($sourceThemes);

        if ($sourceThemes->isEmpty()) {
            $this->info('No branding themes found.');
            return Command::SUCCESS;
        }

        if ($this->option('first')) {
            $sourceThemes = collect([$sourceThemes->first()]);
        }

        $processed = 0;
        $mappedValid = 0;
        $mappedInvalid = 0;
        $recovered = 0;
        $unmatched = 0;

        foreach ($sourceThemes as $sourceTheme) {
            $processed++;

            $sourceId = $sourceTheme['BrandingThemeID'] ?? null;
            $name = trim((string) ($sourceTheme['Name'] ?? ''));

            if (!$sourceId || $name === '') {
                $this->error('Invalid source branding theme.');
                $this->line(json_encode($sourceTheme, JSON_PRETTY_PRINT | JSON_UNESCAPED_SLASHES));
                return Command::FAILURE;
            }

            $mappedId = $destinationRole === 'test'
                ? $mapper->getTestId(self::ENTITY, $sourceId)
                : $mapper->getTargetId(self::ENTITY, $sourceId);

            if ($mappedId) {
                $mappedTheme = $destinationById->get($mappedId);

                if ($mappedTheme && $this->isReasonableMappedMatch($sourceTheme, $mappedTheme)) {
                    $mappedValid++;
                    $this->info("Mapped already → Valid: {$name} ({$mappedId})");
                    continue;
                }

                $mappedInvalid++;
            }

            $matchedDestination = $this->findDestinationMatch($sourceTheme, $destinationByName);

            if ($matchedDestination) {
                $destinationId = $matchedDestination['BrandingThemeID'];

                $this->storeMapping(
                    $mapper,
                    $destinationRole,
                    $source,
                    $destination,
                    $sourceId,
                    $destinationId,
                    $name
                );

                $verifiedId = $destinationRole === 'test'
                    ? $mapper->getTestId(self::ENTITY, $sourceId)
                    : $mapper->getTargetId(self::ENTITY, $sourceId);

                if (!$verifiedId || $verifiedId !== $destinationId) {
                    $this->error("Mapping verification failed for recovered branding theme: {$name}");
                    return Command::FAILURE;
                }

                $recovered++;
                $this->info("Recovered + mapped → {$name} ({$destinationId})");
                continue;
            }

            $unmatched++;
            $this->warn("No destination match found → {$name}");
        }

        $this->newLine();
        $this->info('Done.');
        $this->line("Processed: {$processed}");
        $this->line("Valid mapped: {$mappedValid}");
        $this->line("Invalid mapped found: {$mappedInvalid}");
        $this->line("Recovered + mapped: {$recovered}");
        $this->line("Unmatched: {$unmatched}");

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

            Log::error('Xero branding themes fetch failed', [
                'label' => $label,
                'status' => $response->status(),
                'response' => $response->body(),
            ]);

            throw new \RuntimeException("Failed to fetch {$label} branding themes.");
        }

        $this->error("Failed to fetch {$label} branding themes after retries.");
        throw new \RuntimeException("Failed to fetch {$label} branding themes after retries.");
    }

    private function buildDestinationIndexes(Collection $themes): array
    {
        $byId = $themes
            ->filter(fn ($theme) => !empty($theme['BrandingThemeID']))
            ->keyBy('BrandingThemeID');

        $byName = $themes
            ->filter(fn ($theme) => !empty($theme['Name']))
            ->groupBy(fn ($theme) => $this->normalizeName($theme['Name']));

        return [$byId, $byName];
    }

    private function filterSourceThemes(Collection $themes): Collection
    {
        if ($this->option('source-id')) {
            $sourceId = trim((string) $this->option('source-id'));

            $themes = $themes->filter(function (array $theme) use ($sourceId) {
                return ($theme['BrandingThemeID'] ?? '') === $sourceId;
            })->values();
        }

        if ($this->option('name')) {
            $nameKey = $this->normalizeName((string) $this->option('name'));

            $themes = $themes->filter(function (array $theme) use ($nameKey) {
                return $this->normalizeName($theme['Name'] ?? '') === $nameKey;
            })->values();
        }

        return $themes->values();
    }

    private function findDestinationMatch(array $sourceTheme, Collection $destinationByName): ?array
    {
        $nameKey = $this->normalizeName($sourceTheme['Name'] ?? '');

        if ($nameKey === '') {
            return null;
        }

        $matches = $destinationByName->get($nameKey);

        if ($matches && $matches->count() === 1) {
            return $matches->first();
        }

        return null;
    }

    private function isReasonableMappedMatch(array $sourceTheme, array $destinationTheme): bool
    {
        $sourceName = $this->normalizeName($sourceTheme['Name'] ?? '');
        $destinationName = $this->normalizeName($destinationTheme['Name'] ?? '');

        return $sourceName !== '' && $destinationName !== '' && $sourceName === $destinationName;
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