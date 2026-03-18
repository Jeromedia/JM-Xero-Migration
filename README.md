# Xero Migration

Private Laravel-based console utility for controlled data migration and validation tasks using the Xero API.

⚠️ **Private Project / Private Report**  
This repository and its contents are strictly private and intended for internal use only.

---

## Overview

Xero Migration is a **Laravel console application** designed to run internal automation, integration, or data migration tasks against Xero organizations.

Key characteristics:

- No public web interface
- Executed via Artisan commands only
- Designed for controlled, one-off or batch operations
- Focused on speed, clarity, and minimal infrastructure

---

## Tech Stack

- PHP
- Laravel 12
- MySQL
- Artisan Console Commands
- Xero API (OAuth 2.0)
- Hosted on a small DigitalOcean droplet

---

## Requirements

- PHP ^8.x
- Composer
- Laravel ^12.x
- MySQL
- Xero API credentials with required scopes

---

## Installation

1. Clone the repository

2. Install dependencies

```bash
composer install
```

3. Copy environment file

```bash
cp .env.example .env
```

4. Configure environment variables in `.env`

5. Generate application key

```bash
php artisan key:generate
```

6. Run migrations (if applicable)

```bash
php artisan migrate
```

---

## Configuration

Environment variables are used for all sensitive data and API configuration.

Typical configuration includes:

- Xero API client credentials
- OAuth access and refresh tokens
- Tenant (organization) identifiers
- Logging preferences

No credentials are committed to the repository.

---

## Usage

All functionality is executed via **Artisan commands**.

Example:

```bash
php artisan xero:example-command
```

Commands are designed to be:

- Explicit
- Predictable
- Logged for traceability

Refer to the command class for detailed behavior and options.

---

## Logging & Validation

- Basic logging is enabled for all operations
- Errors are logged using Laravel’s default logging configuration
- Validation focuses on:
  - Record counts
  - Key field consistency
  - API response integrity

Advanced reconciliation, retries, and rollback are intentionally out of scope unless explicitly implemented.

---

## Security

- OAuth 2.0 compliant authentication
- Secrets stored only in environment variables
- No public routes or externally exposed endpoints
- Intended for trusted execution environments only

---

## Scope & Limitations

This project is intentionally minimal.

Unless explicitly implemented, it does **not** include:

- Deduplication logic
- Rollback mechanisms
- Automatic retries
- Data reconciliation reports
- UI or admin dashboards

---

## Development Notes

- Keep commands small and single-purpose
- Prefer clarity over abstraction
- Avoid unnecessary services or listeners
- This is a utility project, not a product

---

## Disclaimer

This project is provided as-is for internal and private use only.  
It assumes correct configuration, permissions, and approvals for the Xero API.
