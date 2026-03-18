<?php

use App\Http\Controllers\XeroAuthController;
use Illuminate\Support\Facades\Route;



Route::view('/', 'welcome');

Route::get('/xero/connect', [XeroAuthController::class, 'connect'])->name('xero.connect');
Route::get('/oauth/callback', [XeroAuthController::class, 'callback'])->name('xero.callback');
