<?php

namespace App\Models;

use Illuminate\Database\Eloquent\Model;

class XeroIdMapping extends Model
{
    protected $table = 'xero_id_mappings';

    protected $fillable = [
        'entity',
        'source_id',
        'test_id',
        'target_id',
        'source_tenant_id',
        'test_tenant_id',
        'target_tenant_id',
        'name',
    ];

    protected $casts = [
        'created_at' => 'datetime',
        'updated_at' => 'datetime',
    ];
}