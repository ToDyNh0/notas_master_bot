# Usage Guide

This guide explains how to configure and use the NF Reader Telegram Bot.

## 1. Environment Configuration

The bot uses a `.env` file to manage secrets without hardcoding them into the source code.
1. Copy the `.env.example` file to create a `.env` file.
2. Provide your bot token (from BotFather) in `BOT_TOKEN`.
3. Provide your Telegram User ID in `ADMIN-AUTH-TOKEN`. This user will be granted full admin permissions.

## 2. Dependencies

To install project dependencies, use the following command:
```bash
pip install -r dependences.txt
```

## 3. Running the Bot

Run the bot script:
```bash
python bot.py
```
The bot will initialize its directories (`output/`, `NF_QR_CODE/`, `nf/`) and start listening for messages.

## 4. User Roles and Commands

There are three roles defined within the application: Admin, Moderator, User.

**Admin commands:**
- `add_user_role`: Grant specific roles to other users.
- `remove_user_role`: Revoke roles from users.
- `view_logs`: View recent bot activity logs.
- `see_users_sending_messages`: View activity by user.
- All Moderator commands.

**Moderator commands:**
- `request_resume`: Request daily, weekly, biweekly, or monthly visual reports. (Limited to 1 report per type per hour).
- `send_nf`: Send NF links/QR codes for parsing. (Limited to 1 note per 50 seconds).
- All User commands.

**User commands:**
- `request_myid`: Find out your exact Telegram ID to give to an admin.
- `help_message`: View the help instructions.

## 5. Submitting Invoices

Once authorized (Moderator or Admin), simply forward the NFC-e URL to the bot. The bot will scrape the NF data from the appropriate SEFAZ portal, respond with an infographic receipt, and log the products securely.
