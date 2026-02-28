# NF Reader Bot

![NF Reader Bot Logo](profile_banenr/Gemini_Generated_Image_gtsx73gtsx73gtsx-removebg-preview.png)

NF Reader is a Telegram bot designed to scan, parse, and organize Brazilian Electronic Invoices (NFC-e) using their QR Code URLs. It stores scanned items, extracted product lists, and offers detailed infographic summaries and reports to track your expenses.

## Features

- Parse NFC-e from Brazilian state finance departments (SEFAZ).
- Store invoice and product information locally in CSV format within a dedicated database directory.
- Generate infographic PNGs and PDF summaries.
- Role-based permissions (Admin, Moderator, User).
- Rate limits to prevent spam (Uploads and Reports).
- Automatically categorizes output files into specific extension-based folders for simple organization.

## Quick Start

1. Install dependencies:
   ```bash
   pip install -r dependences.txt
   ```
2. Set up your environment variables by copying `.env.example` to `.env`. Fill in your `BOT_TOKEN` and your Telegram `ADMIN-AUTH-TOKEN`.
3. Run the bot:
   ```bash
   python bot.py
   ```

## Documentation

For more detailed setup and usage instructions, please refer to our [Usage Guide](USAGE.md).
