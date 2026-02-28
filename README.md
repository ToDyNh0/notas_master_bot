# NF Reader Bot

NF Reader is a Telegram bot designed to scan, parse, and organize Brazilian Electronic Invoices (NFC-e) using their QR Code URLs. It stores scanned items, extracted product lists, and offers detailed infographic summaries/reports to track your expenses.

## Features

- Parse NFC-e from Brazilian state finance departments (SEFAZ).
- Store invoice and product information locally in CSV format.
- Generate infographic PNGs and PDF summaries.
- Role-based permissions (Admin, Moderator, User).
- Rate limits to prevent spam (Uploads and Reports).

## Quick Start

1. Install dependencies:
   ```bash
   pip install -r dependences.txt
   ```
2. Set up your environment variables by copying `.env.example` to `.env` and filling in your `BOT_TOKEN` and your Telegram `ADMIN-AUTH-TOKEN`.
3. Run the bot:
   ```bash
   python bot.py
   ```

## Documentation

For more detailed setup and usage instructions, please refer to [USAGE.md](USAGE.md).
