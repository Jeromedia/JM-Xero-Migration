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
        Schema::create('xero_id_mappings', function (Blueprint $table) {
            $table->id();

            $table->string('entity', 50);

            $table->string('source_id', 100);

            $table->string('test_id', 100)->nullable();
            $table->string('target_id', 100)->nullable();

            $table->string('source_tenant_id', 100);
            $table->string('test_tenant_id', 100)->nullable();
            $table->string('target_tenant_id', 100)->nullable();

            $table->string('name', 255)->nullable();

            $table->timestamps();

            $table->unique([
                'entity',
                'source_id',
                'source_tenant_id',
                'test_tenant_id',
                'target_tenant_id'
            ], 'xero_mapping_unique');
        });
    }

    /**
     * Reverse the migrations.
     */
    public function down(): void
    {
        Schema::dropIfExists('xero_id_mappings');
    }
};
