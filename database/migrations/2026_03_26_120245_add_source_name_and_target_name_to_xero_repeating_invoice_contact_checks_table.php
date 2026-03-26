<?php

use Illuminate\Database\Migrations\Migration;
use Illuminate\Database\Schema\Blueprint;
use Illuminate\Support\Facades\Schema;

return new class extends Migration
{
    public function up(): void
    {
        Schema::table('xero_repeating_invoice_contact_checks', function (Blueprint $table) {
            $table->string('source_name')->nullable()->after('target_contact_id');
            $table->string('target_name')->nullable()->after('source_name');
        });
    }

    public function down(): void
    {
        Schema::table('xero_repeating_invoice_contact_checks', function (Blueprint $table) {
            $table->dropColumn(['source_name', 'target_name']);
        });
    }
};