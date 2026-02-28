# Usage Guide

This guide explains how to properly configure and use the NF Reader Telegram Bot.

## 1. Environment Configuration

The bot uses an environment file to manage secrets without hardcoding them into the source code.
1. Copy the `.env.example` file to create a `.env` file.
2. Provide your bot token (from BotFather) in the `BOT_TOKEN` variable.
3. Provide your Telegram User ID in the `ADMIN-AUTH-TOKEN` variable. This user will be granted full admin permissions.

## 2. Dependencies

To install project dependencies, use the following bash command:
```bash
pip install -r dependences.txt
```

## 3. Running the Bot

Run the bot script:
```bash
python bot.py
```
Upon initialization, the bot will automatically generate all necessary output directories (e.g., `output/pdf`, `output/json`, `output/db`) and begin listening for incoming messages.

## 4. User Roles and Commands

The application defines three basic roles: Admin, Moderator, User.

**Admin commands:**
- `/addrole`: Grant specific roles to other users.
- `/removerole`: Revoke roles from users.
- `/logs`: View recent bot activity logs.
- `/users`: View activity by user.
- All Moderator commands.

**Moderator commands:**
- `/resume [dia|semana|quinzena|mes]`: Request visual reports. (Rate limited to 1 report per type per hour).
- `/nf`: Send NF links or QR codes for parsing. (Rate limited to 1 note per 50 seconds).
- All User commands.

**User commands:**
- `/myid`: Find out your exact Telegram ID to request admin permission.
- `/help`: View basic help instructions.

## 5. Submitting Invoices

Once authorized as a Moderator or Admin, you can simply forward the NFC-e URL or send a photo of the QR code to the bot (use the caption `/nf`). The bot will scrape the NF data from the appropriate SEFAZ portal, respond with an infographic receipt, and securely log the products to the local CSV database.
