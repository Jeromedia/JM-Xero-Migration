# Xero Data Migration Pipeline

This document defines the recommended **migration order for Xero entities** to ensure dependencies are satisfied and API operations succeed.

The order is based on **object dependencies within Xero**.

---

# 1. Organisation & Settings

These entities define the environment and should be verified first.

Entities:

- Organisation
- Users
- BrandingThemes
- Currencies

Example dependency:

Invoice → Currency

---

# 2. Tax Rates

Tax rates are required before creating invoices or bills.

Entities:

- TaxRates

Example dependency:

Invoice Line → TaxType

---

# 3. Chart of Accounts

Accounts are referenced by many financial objects.

Entities:

- Accounts

Example dependency:

Invoice Line → AccountCode
Payment → Account

Important note:

Bank accounts are also stored inside Accounts.
Payments require AccountType = BANK

---

# 4. Tracking Categories

Tracking categories must exist before they can be assigned to transactions.

Entities:

- TrackingCategories
- TrackingOptions

Example dependency:

Invoice Line → TrackingCategory

---

# 5. Contact Groups

Contact groups must be created before assigning contacts to them.

Entities:

- ContactGroups

Example dependency:

Contact → ContactGroups

---

# 6. Contacts

Contacts are required before creating invoices or payments.

Entities:

- Contacts

Example dependency:

Invoice → ContactID
Payment → ContactID

---

# 7. Items

Items are used inside invoice line items.

Entities:

- Items

Example dependency:

Invoice Line → ItemCode

---

# 8. Invoices

Invoices require several previously migrated entities.

Entities:

- Invoices
- CreditNotes
- RepeatingInvoices

Dependencies:

Invoice
├── Contact
├── LineItems
│ ├── Account
│ ├── Item
│ ├── TaxRate
│ └── TrackingCategory
└── Currency

---

# 9. Payments

Payments must be created after invoices.

Entities:

- Payments
- BatchPayments

Dependencies:

Payment
├── Invoice
├── Contact
└── Bank Account

---

# 10. Optional / Advanced Entities

These are not always required but may exist in some organisations.

Entities:

- ManualJournals
- Prepayments
- Overpayments
- PurchaseOrders
- Quotes
- Budgets

These are usually migrated **after the main financial data**.

---

# Complete Recommended Migration Order

1 Organisation
2 Currencies
3 TaxRates
4 Accounts
5 TrackingCategories
6 ContactGroups
7 Contacts
8 Items
9 Invoices
10 CreditNotes
11 RepeatingInvoices
12 Payments
13 BatchPayments

---

# Dependency Summary

Currencies
↓
TaxRates
↓
Accounts
↓
TrackingCategories
↓
ContactGroups
↓
Contacts
↓
Items
↓
Invoices
↓
Payments

---

# Notes for API-Based Migration

- Always migrate **dependency entities first**
- Store **source → destination ID mappings**
- Avoid duplicate creation by checking mappings before POST
- Use **pagination** when reading large datasets
- Consider **batch creation** (up to 50 records per API request)

---

# Recommended Initial Validation Phase

For early API validation, migrate only:

Currencies
TaxRates
Accounts
TrackingCategories
ContactGroups
Contacts
Items

Then validate financial data migration:

Invoices
Payments
