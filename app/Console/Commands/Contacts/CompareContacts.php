<?php

namespace App\Console\Commands\Contacts;

use App\Models\XeroOrganisation;
use Illuminate\Console\Command;
use Illuminate\Http\Client\Response;
use Illuminate\Support\Collection;
use Illuminate\Support\Facades\Http;

class CompareContacts extends Command
{
    protected $signature = 'xero:contacts:compare {--live : Use target instead of test}';
    protected $description = 'Compare contacts between SOURCE and TEST or TARGET';

    public function handle(): int
    {
        $destinationRole = $this->option('live') ? 'target' : 'test';

        $this->info('Starting contacts comparison...');
        $this->info('Source role: source');
        $this->info('Destination role: ' . $destinationRole);

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

        $this->info('Fetching SOURCE contacts...');
        $sourceContacts = $this->fetchAllContacts($source, 'SOURCE');

        $this->info('Fetching ' . strtoupper($destinationRole) . ' contacts...');
        $destinationContacts = $this->fetchAllContacts($destination, strtoupper($destinationRole));

        $sourceKeys = $sourceContacts
            ->map(fn (array $contact) => $this->buildComparisonKey($contact))
            ->filter()
            ->values();

        $destinationKeys = $destinationContacts
            ->map(fn (array $contact) => $this->buildComparisonKey($contact))
            ->filter()
            ->values();

        $matchingCount = $this->countIntersectionWithDuplicates($sourceKeys->all(), $destinationKeys->all());

        $sourceUnique = $sourceKeys->unique()->sort()->values();
        $destinationUnique = $destinationKeys->unique()->sort()->values();

        $missingKeys = $sourceUnique->diff($destinationUnique)->values();
        $extraKeys = $destinationUnique->diff($sourceUnique)->values();

        $missingContacts = $missingKeys
            ->map(fn ($key) => $this->displayFromKey($key))
            ->values();

        $extraContacts = $extraKeys
            ->map(fn ($key) => $this->displayFromKey($key))
            ->values();

        $this->newLine();
        $this->info('Summary');
        $this->line('SOURCE contacts: ' . $sourceContacts->count());
        $this->line(strtoupper($destinationRole) . ' contacts: ' . $destinationContacts->count());

        $this->newLine();
        $this->info('Comparison');
        $this->line('Matching: ' . $matchingCount);
        $this->line('Missing in ' . strtoupper($destinationRole) . ': ' . $missingContacts->count());
        $this->line('Extra in ' . strtoupper($destinationRole) . ': ' . $extraContacts->count());

        if ($missingContacts->isNotEmpty()) {
            $this->newLine();
            $this->warn('Missing contacts:');
            foreach ($missingContacts as $name) {
                $this->line(' - ' . $name);
            }
        }

        if ($extraContacts->isNotEmpty()) {
            $this->newLine();
            $this->warn('Extra contacts:');
            foreach ($extraContacts as $name) {
                $this->line(' - ' . $name);
            }
        }

        $this->newLine();
        $this->info('Comparison complete.');

        return Command::SUCCESS;
    }

    private function fetchAllContacts(XeroOrganisation $organisation, string $label): Collection
    {
        $contacts = collect();
        $page = 1;

        do {
            $response = $this->getContactsPageWithRetry($organisation, $page, $label);

            $data = collect($response->json('Contacts', []));
            $contacts = $contacts->merge($data);
            $page++;
        } while ($data->isNotEmpty());

        return $contacts;
    }

    private function getContactsPageWithRetry(XeroOrganisation $organisation, int $page, string $label): Response
    {
        $maxAttempts = 6;
        $attempt = 1;

        while ($attempt <= $maxAttempts) {
            $response = Http::withToken($organisation->token->access_token)
                ->withHeaders([
                    'Xero-tenant-id' => $organisation->tenant_id,
                    'Accept' => 'application/json',
                ])
                ->get('https://api.xero.com/api.xro/2.0/Contacts', [
                    'page' => $page,
                ]);

            if ($response->successful()) {
                return $response;
            }

            if ($response->status() === 429) {
                $retryAfter = (int) ($response->header('Retry-After') ?? 5);
                $retryAfter = $retryAfter > 0 ? $retryAfter : 5;

                $this->warn("Rate limited while fetching {$label} contacts page {$page}. Waiting {$retryAfter} seconds before retry {$attempt}/{$maxAttempts}...");
                sleep($retryAfter);
                $attempt++;
                continue;
            }

            $this->error("Failed to fetch {$label} contacts.");
            $this->line('HTTP Status: ' . $response->status());
            $this->line($response->body());
            throw new \RuntimeException("Failed to fetch {$label} contacts.");
        }

        $this->error("Failed to fetch {$label} contacts after retries.");
        throw new \RuntimeException("Failed to fetch {$label} contacts after retries.");
    }

    private function buildComparisonKey(array $contact): ?string
    {
        $name = $this->normalizeNameKeepSpaces($contact['Name'] ?? null);

        if (!$name) {
            return null;
        }

        $email = $this->normalizeEmail($contact['EmailAddress'] ?? null);
        $tax = $this->normalizeTax($contact['TaxNumber'] ?? null);

        return implode('|', [
            $name,
            $email ?? '',
            $tax ?? '',
        ]);
    }

    private function normalizeNameKeepSpaces(?string $value): ?string
    {
        if (!$value) {
            return null;
        }

        $value = strtolower($value);
        $value = preg_replace('/\s+/', ' ', trim($value));

        return $value !== '' ? $value : null;
    }

    private function normalizeEmail(?string $value): ?string
    {
        if (!$value) {
            return null;
        }

        $value = strtolower(trim($value));
        return $value !== '' ? $value : null;
    }

    private function normalizeTax(?string $value): ?string
    {
        if (!$value) {
            return null;
        }

        $value = strtolower(trim($value));
        $value = preg_replace('/\s+/', '', $value);

        return $value !== '' ? $value : null;
    }

    private function countIntersectionWithDuplicates(array $sourceKeys, array $destinationKeys): int
    {
        $sourceCounts = array_count_values($sourceKeys);
        $destinationCounts = array_count_values($destinationKeys);

        $matching = 0;

        foreach ($sourceCounts as $key => $sourceCount) {
            if (isset($destinationCounts[$key])) {
                $matching += min($sourceCount, $destinationCounts[$key]);
            }
        }

        return $matching;
    }

    private function displayFromKey(string $key): string
    {
        [$name, $email, $tax] = array_pad(explode('|', $key), 3, '');

        $parts = array_filter([$name, $email, $tax]);

        return implode(' | ', $parts);
    }
}