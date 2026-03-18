<?php

namespace App\Models;

use Illuminate\Database\Eloquent\Model;

class XeroToken extends Model
{
    protected $fillable = [
        'tenant_id',
        'connection_role', // source | target
        'access_token',
        'refresh_token',
        'id_token',
        'scope',
        'expires_at',
    ];
    protected $casts = [
        'expires_at' => 'datetime',
    ];


    public function organisations()
    {
        return $this->hasMany(XeroOrganisation::class, 'xero_token_id');
    }
}
