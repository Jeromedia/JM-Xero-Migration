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
        Schema::create('mapping_xero_contact_group', function (Blueprint $table) {
            $table->id();

            $table->string('entity', 50);

            $table->string('source_id', 100);
            $table->string('test_id', 100)->nullable();
            $table->string('target_id', 100)->nullable();

            $table->string('name', 255)->nullable();

            $table->string('source_tenant_id', 100);
            $table->string('test_tenant_id', 100)->nullable();
            $table->string('target_tenant_id', 100)->nullable();

            $table->timestamps();

            $table->unique(
                ['entity', 'source_id', 'source_tenant_id'],
                'xero_contact_mappings_source_unique'
            );
        });
    }

    /**
     * Reverse the migrations.
     */
    public function down(): void
    {
        Schema::dropIfExists('mapping_xero_contact_group');
    }
};
