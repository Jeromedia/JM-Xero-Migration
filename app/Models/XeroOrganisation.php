<?php

namespace App\Models;

use Illuminate\Database\Eloquent\Model;

class XeroOrganisation extends Model
{
    protected $fillable = [
        'xero_token_id',
        'tenant_id',
        'tenant_name',
        'tenant_type',
        'role', // optional: source|target
    ];

    public function token()
    {
        return $this->belongsTo(XeroToken::class, 'xero_token_id');
    }
}