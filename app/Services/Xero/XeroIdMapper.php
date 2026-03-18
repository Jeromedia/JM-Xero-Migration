<?php

namespace App\Services\Xero;

use App\Models\XeroIdMapping;

class XeroIdMapper
{
    public function getTestId(string $entity, string $sourceId): ?string
    {
        return XeroIdMapping::where('entity', $entity)
            ->where('source_id', $sourceId)
            ->value('test_id');
    }

    public function getTargetId(string $entity, string $sourceId): ?string
    {
        return XeroIdMapping::where('entity', $entity)
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

        XeroIdMapping::updateOrCreate(
            [
                'entity' => $entity,
                'source_id' => $sourceId
            ],
            [
                'test_id' => $testId,
                'source_tenant_id' => $sourceTenant,
                'test_tenant_id' => $testTenant,
                'name' => $name
            ]
        );
    }

    public function storeTarget(
        string $entity,
        string $sourceId,
        string $targetId,
        string $sourceTenant,
        string $targetTenant
    ): void {

        XeroIdMapping::updateOrCreate(
            [
                'entity' => $entity,
                'source_id' => $sourceId
            ],
            [
                'target_id' => $targetId,
                'source_tenant_id' => $sourceTenant,
                'target_tenant_id' => $targetTenant
            ]
        );
    }
}