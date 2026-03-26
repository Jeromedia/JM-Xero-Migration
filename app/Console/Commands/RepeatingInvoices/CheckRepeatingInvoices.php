<?php

namespace App\Console\Commands\RepeatingInvoices;

use App\Models\XeroOrganisation;
use App\Models\XeroToken;
use Carbon\Carbon;
use Illuminate\Console\Command;
use Illuminate\Support\Facades\Http;
use Throwable;

class CheckRepeatingInvoices extends Command
{
    protected $signature = 'xero:repeating-invoices:check';
    protected $description = 'Show a detailed breakdown of repeating invoice templates in the SOURCE Xero organisation';

    public function handle(): int
    {
        $this->info('Starting repeating invoices check...');
        $this->line('Source role: source');

        $token = XeroToken::orderByDesc('id')->first();

        if (!$token) {
            $this->error('No Xero token found.');
            return self::FAILURE;
        }

        $sourceOrganisation = XeroOrganisation::where('role', 'source')->first();

        if (!$sourceOrganisation) {
            $this->error('No SOURCE organisation found.');
            return self::FAILURE;
        }

        $tenantId = $sourceOrganisation->xero_tenant_id
            ?? $sourceOrganisation->tenant_id
            ?? null;

        if (!$tenantId) {
            $this->error('No tenant ID found on the SOURCE organisation.');
            $this->line('Checked fields: xero_tenant_id, tenant_id');
            $this->line(json_encode($sourceOrganisation->toArray(), JSON_PRETTY_PRINT));
            return self::FAILURE;
        }

        try {
            $this->line('Fetching repeating invoices from SOURCE...');

            $response = Http::withToken($token->access_token)
                ->withHeaders([
                    'Xero-tenant-id' => $tenantId,
                    'Accept' => 'application/json',
                ])
                ->get('https://api.xero.com/api.xro/2.0/RepeatingInvoices');

            if ($response->failed()) {
                $this->error('Failed to fetch repeating invoices.');
                $this->line('HTTP Status: ' . $response->status());
                $this->line($response->body());

                return self::FAILURE;
            }

            $data = $response->json();
            $repeatingInvoices = $data['RepeatingInvoices'] ?? [];

            $today = Carbon::today();

            $total = count($repeatingInvoices);

            // Status buckets
            $statusCounts = [];
            $deletedCount = 0;
            $draftCount = 0;
            $authorisedCount = 0;
            $otherStatusCount = 0;

            // Schedule buckets (NON-DELETED only)
            $nonDeletedCount = 0;
            $futureScheduledCount = 0;
            $endedByEndDateCount = 0;
            $pastNextScheduledDateCount = 0;
            $noNextScheduledDateCount = 0;
            $noEndDateCount = 0;
            $activeOpenEndedCount = 0;
            $activeWithEndDateCount = 0;
            $inactiveUndeterminedCount = 0;

            // Optional extra buckets
            $typeCounts = [];
            $scheduleUnitCounts = [];

            foreach ($repeatingInvoices as $invoice) {
                $status = strtoupper(trim((string)($invoice['Status'] ?? 'UNKNOWN')));
                $type = strtoupper(trim((string)($invoice['Type'] ?? 'UNKNOWN')));

                $statusCounts[$status] = ($statusCounts[$status] ?? 0) + 1;
                $typeCounts[$type] = ($typeCounts[$type] ?? 0) + 1;

                if ($status === 'DELETED') {
                    $deletedCount++;
                    continue;
                }

                $nonDeletedCount++;

                if ($status === 'DRAFT') {
                    $draftCount++;
                } elseif ($status === 'AUTHORISED') {
                    $authorisedCount++;
                } else {
                    $otherStatusCount++;
                }

                $schedule = $invoice['Schedule'] ?? [];

                $nextScheduledDateRaw = $schedule['NextScheduledDate'] ?? null;
                $endDateRaw = $schedule['EndDate'] ?? null;
                $unit = strtoupper(trim((string)($schedule['Unit'] ?? 'UNKNOWN')));

                $scheduleUnitCounts[$unit] = ($scheduleUnitCounts[$unit] ?? 0) + 1;

                $nextDate = $this->parseXeroDate($nextScheduledDateRaw);
                $endDate = $this->parseXeroDate($endDateRaw);

                if (!$nextDate) {
                    $noNextScheduledDateCount++;
                }

                if (!$endDate) {
                    $noEndDateCount++;
                }

                $isEndedByEndDate = $endDate && $endDate->lt($today);
                $isPastNextScheduledDate = $nextDate && $nextDate->lt($today);
                $isFutureScheduled = $nextDate && $nextDate->gte($today) && (!$endDate || $endDate->gte($today));

                if ($isEndedByEndDate) {
                    $endedByEndDateCount++;
                }

                if ($isPastNextScheduledDate) {
                    $pastNextScheduledDateCount++;
                }

                if ($isFutureScheduled) {
                    $futureScheduledCount++;

                    if ($endDate) {
                        $activeWithEndDateCount++;
                    } else {
                        $activeOpenEndedCount++;
                    }
                }

                // Catch non-deleted rows that don't fall neatly into future-scheduled
                if (!$isFutureScheduled && !$isEndedByEndDate && !$isPastNextScheduledDate) {
                    $inactiveUndeterminedCount++;
                }
            }

            arsort($statusCounts);
            arsort($typeCounts);
            arsort($scheduleUnitCounts);

            $endedOrNoLongerFutureCount = $nonDeletedCount - $futureScheduledCount;

            $this->newLine();
            $this->info('=== Repeating Invoices Summary ===');
            $this->line('SOURCE tenant: ' . ($sourceOrganisation->tenant_name ?? $sourceOrganisation->name ?? 'Unknown'));
            $this->line('SOURCE tenant ID: ' . $tenantId);

            $this->newLine();
            $this->info('=== Key Numbers ===');
            $this->table(
                ['Metric', 'Count'],
                [
                    ['Total templates returned by API', $total],
                    ['Deleted templates', $deletedCount],
                    ['Non-deleted templates', $nonDeletedCount],
                    ['Future scheduled templates (best active number)', $futureScheduledCount],
                    ['Non-deleted but not future scheduled', $endedOrNoLongerFutureCount],
                ]
            );

            $this->newLine();
            $this->info('=== Status Breakdown (all templates) ===');

            $statusRows = [];
            foreach ($statusCounts as $status => $count) {
                $statusRows[] = [$status, $count];
            }
            $this->table(['Status', 'Count'], $statusRows);

            $this->newLine();
            $this->info('=== Schedule Breakdown (non-deleted only) ===');
            $this->table(
                ['Metric', 'Count'],
                [
                    ['Non-deleted templates', $nonDeletedCount],
                    ['Future scheduled templates', $futureScheduledCount],
                    ['Ended by EndDate < today', $endedByEndDateCount],
                    ['Past NextScheduledDate < today', $pastNextScheduledDateCount],
                    ['No NextScheduledDate', $noNextScheduledDateCount],
                    ['No EndDate', $noEndDateCount],
                    ['Active open-ended templates', $activeOpenEndedCount],
                    ['Active templates with end date', $activeWithEndDateCount],
                    ['Undetermined non-deleted templates', $inactiveUndeterminedCount],
                ]
            );

            $this->newLine();
            $this->info('=== Non-Deleted Status Breakdown ===');
            $this->table(
                ['Metric', 'Count'],
                [
                    ['Draft', $draftCount],
                    ['Authorised', $authorisedCount],
                    ['Other non-deleted statuses', $otherStatusCount],
                ]
            );

            if (!empty($typeCounts)) {
                $this->newLine();
                $this->info('=== Type Breakdown (all templates) ===');

                $typeRows = [];
                foreach ($typeCounts as $type => $count) {
                    $typeRows[] = [$type, $count];
                }

                $this->table(['Type', 'Count'], $typeRows);
            }

            if (!empty($scheduleUnitCounts)) {
                $this->newLine();
                $this->info('=== Schedule Unit Breakdown (non-deleted only) ===');

                $unitRows = [];
                foreach ($scheduleUnitCounts as $unit => $count) {
                    $unitRows[] = [$unit, $count];
                }

                $this->table(['Unit', 'Count'], $unitRows);
            }

            return self::SUCCESS;
        } catch (Throwable $e) {
            $this->error('Unexpected error while checking repeating invoices.');
            $this->line($e->getMessage());

            return self::FAILURE;
        }
    }

    private function parseXeroDate(?string $value): ?Carbon
    {
        if (!$value) {
            return null;
        }

        try {
            return Carbon::parse($value)->startOfDay();
        } catch (Throwable $e) {
            if (preg_match('/\/Date\((\d+)/', $value, $matches)) {
                return Carbon::createFromTimestampMs((int) $matches[1])->startOfDay();
            }

            return null;
        }
    }
}