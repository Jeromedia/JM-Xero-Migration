<?php

namespace App\Models\Mapping;

use Illuminate\Database\Eloquent\Model;

class XeroContactMapping extends Model
{
    protected $table = 'mapping_xero_contact';
    protected $fillable = [
        'entity',
        'source_id',
        'test_id',
        'target_id',
        'name',
        'source_tenant_id',
        'test_tenant_id',
        'target_tenant_id',
    ];
}
