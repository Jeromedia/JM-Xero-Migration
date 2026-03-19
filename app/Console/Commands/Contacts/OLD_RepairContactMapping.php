<?php

namespace App\Console\Commands\Contacts;

use App\Models\XeroOrganisation;
use App\Services\Xero\XeroIdMapper;
use Illuminate\Console\Command;
use Illuminate\Support\Facades\Http;

class RepairContactMapping extends Command
{
    protected $signature = 'xero:contacts:repair-mapping {--live : Use target instead of test}';
    protected $description = 'Repair contact mappings using smart matching';

    private const ENTITY = 'contact';

    public function handle(): int
    {
        $destinationRole = $this->option('live') ? 'target' : 'test';

        $this->info('Starting contact mapping repair...');

        $mapper = new XeroIdMapper();

        $source = XeroOrganisation::where('role', 'source')->first();
        $destination = XeroOrganisation::where('role', $destinationRole)->first();

        if (!$source || !$destination) {
            $this->error('Source or destination not found.');
            return Command::FAILURE;
        }

        // 🔹 NORMALIZER (STRONG)
        $normalize = function ($name) {
            $name = strtolower($name);
            $name = preg_replace('/[^a-z0-9]/', '', $name);
            return $name;
        };

        // 🔹 FETCH ALL SOURCE CONTACTS
        $this->info('Fetching SOURCE contacts...');
        $sourceContacts = collect();
        $page = 1;

        do {
            $response = Http::withToken($source->token->access_token)
                ->withHeaders([
                    'Xero-tenant-id' => $source->tenant_id,
                    'Accept' => 'application/json',
                ])
                ->get('https://api.xero.com/api.xro/2.0/Contacts', [
                    'page' => $page
                ]);

            if (!$response->successful()) {
                $this->error('Failed SOURCE fetch');
                return Command::FAILURE;
            }

            $data = collect($response->json('Contacts', []));
            $sourceContacts = $sourceContacts->merge($data);

            $page++;

        } while ($data->isNotEmpty());

        // 🔹 FETCH ALL DESTINATION CONTACTS
        $this->info('Fetching DESTINATION contacts...');
        $destinationContacts = collect();
        $page = 1;

        do {
            $response = Http::withToken($destination->token->access_token)
                ->withHeaders([
                    'Xero-tenant-id' => $destination->tenant_id,
                    'Accept' => 'application/json',
                ])
                ->get('https://api.xero.com/api.xro/2.0/Contacts', [
                    'page' => $page
                ]);

            if (!$response->successful()) {
                $this->error('Failed DESTINATION fetch');
                return Command::FAILURE;
            }

            $data = collect($response->json('Contacts', []));
            $destinationContacts = $destinationContacts->merge($data);

            $page++;

        } while ($data->isNotEmpty());

        $this->info("SOURCE total: " . $sourceContacts->count());
        $this->info("DESTINATION total: " . $destinationContacts->count());

        // 🔹 BUILD INDEXES
        $destinationByName = $destinationContacts->groupBy(function ($c) use ($normalize) {
            return $normalize($c['Name'] ?? '');
        });

        $destinationByEmail = $destinationContacts
            ->filter(fn ($c) => !empty($c['EmailAddress']))
            ->groupBy(fn ($c) => strtolower(trim($c['EmailAddress'])));

        $destinationByTax = $destinationContacts
            ->filter(fn ($c) => !empty($c['TaxNumber']))
            ->groupBy(fn ($c) => preg_replace('/\s+/', '', $c['TaxNumber']));

        $mapped = 0;
        $skipped = 0;
        $processed = 0;
        $total = $sourceContacts->count();

        $this->info("Processing {$total} contacts...");

        // 🔹 PROGRESS BAR
        $bar = $this->output->createProgressBar($total);
        $bar->start();

        foreach ($sourceContacts as $sourceContact) {

            $processed++;

            $sourceId = $sourceContact['ContactID'];
            $name = $sourceContact['Name'] ?? null;

            if (!$name) {
                $skipped++;
                $bar->advance();
                continue;
            }

            // 🔹 SKIP IF ALREADY MAPPED
            $existing = $destinationRole === 'test'
                ? $mapper->getTestId(self::ENTITY, $sourceId)
                : $mapper->getTargetId(self::ENTITY, $sourceId);

            if ($existing) {
                $skipped++;
                $bar->advance();
                continue;
            }

            $normalized = $normalize($name);

            // 1️⃣ NAME MATCH
            $matches = $destinationByName->get($normalized);

            // 2️⃣ EMAIL MATCH
            if ((!$matches || $matches->count() !== 1) && !empty($sourceContact['EmailAddress'])) {

                $emailKey = strtolower(trim($sourceContact['EmailAddress']));
                $matches = $destinationByEmail->get($emailKey);
            }

            // 3️⃣ TAX MATCH
            if ((!$matches || $matches->count() !== 1) && !empty($sourceContact['TaxNumber'])) {

                $taxKey = preg_replace('/\s+/', '', $sourceContact['TaxNumber']);
                $matches = $destinationByTax->get($taxKey);
            }

            // ❌ NOT SAFE
            if (!$matches || $matches->count() !== 1) {
                $skipped++;
                $bar->advance();
                continue;
            }

            $destinationContact = $matches->first();
            $destinationId = $destinationContact['ContactID'];

            // 🔹 STORE MAPPING
            if ($destinationRole === 'test') {
                $mapper->storeTest(
                    self::ENTITY,
                    $sourceId,
                    $destinationId,
                    $source->tenant_id,
                    $destination->tenant_id,
                    $name
                );
            } else {
                $mapper->storeTarget(
                    self::ENTITY,
                    $sourceId,
                    $destinationId,
                    $source->tenant_id,
                    $destination->tenant_id,
                    $name
                );
            }

            $mapped++;

            // 🔹 LIGHT STATUS EVERY 100
            if ($processed % 100 === 0) {
                $this->line("\nProcessed: {$processed} | Mapped: {$mapped} | Skipped: {$skipped}");
            }

            $bar->advance();
        }

        $bar->finish();
        $this->newLine(2);

        $this->info("Done.");
        $this->line("Mapped: {$mapped}");
        $this->line("Skipped: {$skipped}");

        return Command::SUCCESS;
    }
}