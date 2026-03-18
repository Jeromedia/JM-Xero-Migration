<?php

use Illuminate\Database\Migrations\Migration;
use Illuminate\Database\Schema\Blueprint;
use Illuminate\Support\Facades\Schema;

return new class extends Migration {
    public function up(): void
    {
        Schema::create('xero_tokens', function (Blueprint $table) {
            $table->id();

            $table->text('access_token');
            $table->text('refresh_token');
            $table->text('id_token')->nullable();
            $table->text('scope')->nullable();
            $table->timestamp('expires_at')->nullable();

            $table->timestamps();
        });
    }

    public function down(): void
    {
        Schema::dropIfExists('xero_tokens');
    }
};