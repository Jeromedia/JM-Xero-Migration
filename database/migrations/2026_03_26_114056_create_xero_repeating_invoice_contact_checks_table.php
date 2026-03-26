<?php

use Illuminate\Database\Migrations\Migration;
use Illuminate\Database\Schema\Blueprint;
use Illuminate\Support\Facades\Schema;

return new class extends Migration
{
    /**
     * Run the migrations.
     */
    public function up(): void
    {
        Schema::create('xero_repeating_invoice_contact_checks', function (Blueprint $table) {
            $table->id();

            $table->unsignedBigInteger('xero_repeating_invoice_check_id')->nullable();
            $table->string('source_contact_id')->nullable();
            $table->string('target_contact_id')->nullable();

            $table->string('result')->nullable();
            $table->text('message')->nullable();
            $table->timestamp('checked_at')->nullable();

            $table->timestamps();

            $table->index('xero_repeating_invoice_check_id', 'xricc_check_id_idx');
            $table->index('source_contact_id', 'xricc_source_contact_idx');
            $table->index('target_contact_id', 'xricc_target_contact_idx');

            // Optional foreign key
            // $table->foreign('xero_repeating_invoice_check_id', 'xricc_check_id_fk')
            //     ->references('id')
            //     ->on('xero_repeating_invoice_checks')
            //     ->nullOnDelete();
        });
    }

    /**
     * Reverse the migrations.
     */
    public function down(): void
    {
        Schema::dropIfExists('xero_repeating_invoice_contact_checks');
    }
};
