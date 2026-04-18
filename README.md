# Budhub

A lightweight dispensary ordering workflow app built with Python standard library and SQLite.

## Features

- Customer bag checkout that creates one grouped ticket
- Role-based logins for admin, in-house bank, dispatch, picker, driver, and customer
- Payment verification can happen later in the flow, but delivery cannot be completed until it is verified
- Dispatcher-controlled driver assignment, pull-backs, and cancel reasons
- Picker review flow for out-of-stock substitutions instead of hard canceling

## Run

```powershell
python app.py
```

Open `http://127.0.0.1:8000`

## Demo Accounts

- Admin: `admin@ecommerce.local` / `admin123`
- Budhub Helpdesk Engineer: `helpdesk@ecommerce.local` / `helpdesk123`
- Bank: `bank@ecommerce.local` / `bank123`
- Dispatcher: `dispatcher@ecommerce.local` / `dispatch123`
- Picker: `picker@ecommerce.local` / `picker123`
- Driver: `driver@ecommerce.local` / `driver123`
- Client: `client@ecommerce.local` / `client123`
