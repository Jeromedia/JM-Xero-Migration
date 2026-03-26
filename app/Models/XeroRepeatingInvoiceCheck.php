<?php

namespace App\Models;

use Illuminate\Database\Eloquent\Model;

class XeroRepeatingInvoiceCheck extends Model
{
    protected $table = 'xero_repeating_invoice_checks';

    protected $fillable = [
        'mapping_id',
        'source_id',
        'target_id',
        'result',
        'message',
        'source_contact_id',
        'target_contact_id',
        'source_contact_name',
        'target_contact_name',
        'source_total',
        'target_total',
        'source_schedule',
        'target_schedule',
        'source_account_codes',
        'target_account_codes',
        'checked_at',
    ];

    protected $casts = [
        'source_total' => 'decimal:4',
        'target_total' => 'decimal:4',
        'source_schedule' => 'array',
        'target_schedule' => 'array',
        'source_account_codes' => 'array',
        'target_account_codes' => 'array',
        'checked_at' => 'datetime',
    ];
}