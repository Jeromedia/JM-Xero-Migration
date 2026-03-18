<?php

use Illuminate\Database\Migrations\Migration;
use Illuminate\Database\Schema\Blueprint;
use Illuminate\Support\Facades\Schema;

return new class extends Migration {
    public function up(): void
    {
        Schema::create('xero_organisations', function (Blueprint $table) {
            $table->id();

            $table->foreignId('xero_token_id')
                ->constrained('xero_tokens')
                ->cascadeOnDelete();

            $table->string('tenant_id')->index();      // Xero tenantId (GUID)
            $table->string('tenant_name')->nullable(); // tenantName
            $table->string('tenant_type')->nullable(); // tenantType

            // optional but useful for your migration project
            $table->string('role')->nullable()->index(); // source|target

            $table->timestamps();

            $table->unique(['xero_token_id', 'tenant_id']);
            $table->unique(['role']); // ensures only 1 source + 1 target (remove if you don’t want)
        });
    }

    public function down(): void
    {
        Schema::dropIfExists('xero_organisations');
    }
};