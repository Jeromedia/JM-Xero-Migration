<?php

namespace App\Services\Xero;

use App\Models\Mapping\XeroAccountMapping;
use App\Models\Mapping\XeroBrandingThemeMapping;
use App\Models\Mapping\XeroContactGroupMapping;
use App\Models\Mapping\XeroContactMapping;
use App\Models\Mapping\XeroCreditNoteMapping;
use App\Models\Mapping\XeroCurrencyMapping;
use App\Models\Mapping\XeroInvoiceMapping;
use App\Models\Mapping\XeroItemMapping;
use App\Models\Mapping\XeroPaymentMapping;
use App\Models\Mapping\XeroRepeatingInvoiceMapping;
use App\Models\Mapping\XeroTaxRateMapping;
use App\Models\Mapping\XeroTrackingCategoryMapping;
use App\Models\Mapping\XeroTrackingOptionMapping;
use InvalidArgumentException;

class XeroIdMapper
{
    public function getTestId(string $entity, string $sourceId): ?string
    {
        $model = $this->resolveModel($entity);

        return $model::where('entity', $entity)
            ->where('source_id', $sourceId)
            ->value('test_id');
    }

    public function getTargetId(string $entity, string $sourceId): ?string
    {
        $model = $this->resolveModel($entity);

        return $model::where('entity', $entity)
            ->where('source_id', $sourceId)
            ->value('target_id');
    }

    public function storeTest(
        string $entity,
        string $sourceId,
        string $testId,
        string $sourceTenant,
        string $testTenant,
        ?string $name = null
    ): void {
        $model = $this->resolveModel($entity);

        $model::updateOrCreate(
            [
                'entity' => $entity,
                'source_id' => $sourceId,
            ],
            [
                'test_id' => $testId,
                'source_tenant_id' => $sourceTenant,
                'test_tenant_id' => $testTenant,
                'name' => $name,
            ]
        );
    }

    public function storeTarget(
        string $entity,
        string $sourceId,
        string $targetId,
        string $sourceTenant,
        string $targetTenant,
        ?string $name = null
    ): void {
        $model = $this->resolveModel($entity);

        $model::updateOrCreate(
            [
                'entity' => $entity,
                'source_id' => $sourceId,
            ],
            [
                'target_id' => $targetId,
                'source_tenant_id' => $sourceTenant,
                'target_tenant_id' => $targetTenant,
                'name' => $name,
            ]
        );
    }

    private function resolveModel(string $entity): string
    {
        return match ($entity) {
            'currency' => XeroCurrencyMapping::class,
            'tax_rate' => XeroTaxRateMapping::class,
            'account' => XeroAccountMapping::class,
            'tracking_category' => XeroTrackingCategoryMapping::class,
            'tracking_option' => XeroTrackingOptionMapping::class,
            'branding_theme' => XeroBrandingThemeMapping::class,
            'contact_group' => XeroContactGroupMapping::class,
            'contact' => XeroContactMapping::class,
            'item' => XeroItemMapping::class,
            'invoice' => XeroInvoiceMapping::class,
            'credit_note' => XeroCreditNoteMapping::class,
            'payment' => XeroPaymentMapping::class,
            'repeating_invoice' => XeroRepeatingInvoiceMapping::class,
            default => throw new InvalidArgumentException("Unsupported Xero mapping entity: {$entity}"),
        };
    }
}