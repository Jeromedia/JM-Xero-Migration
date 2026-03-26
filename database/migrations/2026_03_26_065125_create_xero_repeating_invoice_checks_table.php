<?php

use Illuminate\Database\Migrations\Migration;
use Illuminate\Database\Schema\Blueprint;
use Illuminate\Support\Facades\Schema;

return new class extends Migration
{
    public function up(): void
    {
        Schema::create('xero_repeating_invoice_checks', function (Blueprint $table) {
            $table->id();
            $table->unsignedBigInteger('mapping_id')->unique();
            $table->uuid('source_id')->index();
            $table->uuid('target_id')->index();
            $table->string('result', 20);
            $table->text('message')->nullable();
            $table->uuid('source_contact_id')->nullable();
            $table->uuid('target_contact_id')->nullable();
            $table->string('source_contact_name')->nullable();
            $table->string('target_contact_name')->nullable();
            $table->decimal('source_total', 15, 4)->nullable();
            $table->decimal('target_total', 15, 4)->nullable();
            $table->json('source_schedule')->nullable();
            $table->json('target_schedule')->nullable();
            $table->json('source_account_codes')->nullable();
            $table->json('target_account_codes')->nullable();
            $table->timestamp('checked_at')->nullable();
            $table->timestamps();

            $table->index(['source_id', 'target_id'], 'xric_source_target_idx');
        });
    }

    public function down(): void
    {
        Schema::dropIfExists('xero_repeating_invoice_checks');
    }
};