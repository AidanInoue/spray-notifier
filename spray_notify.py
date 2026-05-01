"""
Spray Daily Notification Script
Scrapes the Geneva Pesticide Application System (https://gpas.cals.cornell.edu/web_post) and sends a daily email summary for specified field blocks, flagging new sprays, scheduled applications,
and active/upcoming REI periods.

Requires GitHub Actions secrets:
  GMAIL_USER     - sender Gmail address
  GMAIL_PASS     - Gmail App Password (not your account password)
  NOTIFY_EMAIL   - recipient email address (can be same as GMAIL_USER)
"""

import os
import json
import smtplib
import sys
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from pathlib import Path

import requests
from bs4 import BeautifulSoup

# ── Configuration ─────────────────────────────────────────────────────────────

GPAS_URL = "https://gpas.cals.cornell.edu/web_post"
#### INPUT FIELDS NOTIFIED HERE ####
# Blocks you work in — edit this list to match your fields.
# The block code is the first part of the block ID, e.g. "DA008APLMAK" → "DA008APLMAK"
# You can use partial matches: "DA008" will match any block starting with DA008.
# MWO Added here to include all of Maddy's field sites.
MY_BLOCKS = [
    "DA001GRPSJK",
    "MWO",
    # Add more blocks here as needed, e.g.:
    # "DA022RASCAW",
]

# REI warning threshold: alert if REI expires within this many hours from now
REI_WARN_HOURS = 24

# Path for caching seen record IDs so we only alert on new entries
SEEN_FILE = Path("data/seen_records.json")

# ── Scraping ──────────────────────────────────────────────────────────────────

def fetch_records():
    """Fetch all web post records from GPAS."""
    headers = {
        "User-Agent": (
            "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 "
            "(KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
        ),
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
        "Accept-Language": "en-US,en;q=0.5",
    }
    resp = requests.get(GPAS_URL, headers=headers, timeout=30)
    resp.raise_for_status()
    soup = BeautifulSoup(resp.text, "html.parser")

    table = soup.find("table")
    if not table:
        raise RuntimeError("Could not find data table on GPAS page.")

    headers_row = [th.get_text(strip=True) for th in table.find_all("th")]
    records = []
    for tr in table.find_all("tr")[1:]:  # skip header row
        cells = [td.get_text(strip=True) for td in tr.find_all("td")]
        if len(cells) == len(headers_row):
            records.append(dict(zip(headers_row, cells)))

    return records


def parse_datetime(s):
    """Parse a GPAS datetime string, return None if not parseable."""
    if not s or s.strip() in ("N/A", ""):
        return None
    for fmt in ("%m/%d/%Y %H:%M:%S", "%m/%d/%Y %H:%M"):
        try:
            return datetime.strptime(s.strip(), fmt).replace(tzinfo=ET)
        except ValueError:
            continue
    return None


def parse_rei_hours(s):
    """Extract REI hours from strings like '48.0 hours after application'."""
    if not s:
        return None
    try:
        return float(s.split()[0])
    except (ValueError, IndexError):
        return None

# ── Filtering ─────────────────────────────────────────────────────────────────

def is_my_block(block_id):
    """Return True if the block matches any of the configured blocks."""
    return any(block_id.startswith(b) or b in block_id for b in MY_BLOCKS)


def load_seen():
    if SEEN_FILE.exists():
        data = json.loads(SEEN_FILE.read_text())
        if isinstance(data, list):
            return set(data)
        return set(data.get("seen", []))
    return set()


def save_seen(seen, mark_digest_today=False):
    SEEN_FILE.parent.mkdir(parents=True, exist_ok=True)
    existing = json.loads(SEEN_FILE.read_text()) if SEEN_FILE.exists() else {}
    last_digest = existing.get("last_digest_date", "") if isinstance(existing, dict) else ""
    if mark_digest_today:
        last_digest = datetime.now(ET).strftime("%Y-%m-%d")
    SEEN_FILE.write_text(json.dumps({
        "seen": sorted(seen),
        "last_digest_date": last_digest
    }))

# ── Analysis ──────────────────────────────────────────────────────────────────

def analyze_records(records, now):
    """
    Returns dicts of:
      new_sprays    - records for my blocks not previously seen
      active_reis   - my block records where REI is currently in effect
      upcoming_reis - my block records where REI expires within REI_WARN_HOURS
      scheduled     - my block records with status 'Sched'
    """
    seen = load_seen()
    new_seen = set(seen)

    new_sprays = []
    active_reis = []
    upcoming_reis = []
    scheduled = []

    for r in records:
        block = r.get("Block", "")
        if not is_my_block(block):
            continue

        rec_id = r.get("Record #", "")
        status = r.get("Status", "")
        reentry_date_str = r.get("Reentry Date", "")
        reentry_dt = parse_datetime(reentry_date_str)

        # New spray detection
        if rec_id and rec_id not in seen:
            new_sprays.append(r)
            new_seen.add(rec_id)

        # Scheduled sprays
        if status.lower() == "sched":
            scheduled.append(r)
# REI analysis — only for records that have actually been executed
        if reentry_dt and status.lower() != "sched":
            if reentry_dt > now:
                hours_remaining = (reentry_dt - now).total_seconds() / 3600
                if hours_remaining <= REI_WARN_HOURS:
                    upcoming_reis.append((r, hours_remaining))
            # Still within REI right now
            if now < reentry_dt:
                active_reis.append((r, reentry_dt))

    save_seen(new_seen)
    return new_sprays, active_reis, upcoming_reis, scheduled

# ── Email ─────────────────────────────────────────────────────────────────────

def fmt_record(r, extra=""):
    """Format a single record for the email digest."""
    return (
        f"  Block:     {r.get('Block', 'N/A')}\n"
        f"  Products:  {r.get('Products', 'N/A')}\n"
        f"  Applied:   {r.get('Date Started', 'N/A')} → {r.get('Date Finished', 'N/A')}\n"
        f"  REI:       {r.get('Reentry Interval', 'N/A')}\n"
        f"  REI Ends:  {r.get('Reentry Date', 'N/A')}\n"
        + (f"  Note:      {extra}\n" if extra else "")
    )

def build_email_body(now, new_sprays, active_reis, upcoming_reis, scheduled):
    lines = []
    lines.append(f"GPAS Daily Digest — {now.strftime('%A, %B %d, %Y %I:%M %p')}")
    lines.append(f"Monitoring blocks: {', '.join(MY_BLOCKS)}")
    lines.append("=" * 60)

    if not any([new_sprays, active_reis, upcoming_reis, scheduled]):
        lines.append("\n✅ No activity or REI concerns for your blocks today.")
        lines.append(f"\nFull records: {GPAS_URL}")
        return "\n".join(lines)

    if active_reis:
        lines.append(f"\n🚫 ACTIVE REI — DO NOT ENTER ({len(active_reis)} block(s)):\n")
        for r, rei_end in active_reis:
            hrs = (rei_end - now).total_seconds() / 3600
            lines.append(fmt_record(r, extra=f"REI expires in {hrs:.1f} hours"))

    if upcoming_reis:
        lines.append(f"\n⏰ REI EXPIRING SOON — within {REI_WARN_HOURS}h ({len(upcoming_reis)} block(s)):\n")
        for r, hrs in upcoming_reis:
            lines.append(fmt_record(r, extra=f"Clears in {hrs:.1f} hours"))

    if new_sprays:
        lines.append(f"\n🆕 NEW SPRAY RECORDS ({len(new_sprays)} new since last check):\n")
        for r in new_sprays:
            lines.append(fmt_record(r))

    if scheduled:
        lines.append(f"\n📅 SCHEDULED (not yet executed) ({len(scheduled)} block(s)):\n")
        for r in scheduled:
            lines.append(fmt_record(r))

    lines.append(f"\nFull records: {GPAS_URL}")
    return "\n".join(lines)


def send_email(subject, body):
    gmail_user = os.environ["GMAIL_USER"]
    gmail_pass = os.environ["GMAIL_PASS"]
    notify_email = os.environ["NOTIFY_EMAIL"]

    msg = MIMEMultipart("alternative")
    msg["Subject"] = subject
    msg["From"] = gmail_user
    msg["To"] = notify_email
    msg.attach(MIMEText(body, "plain"))

    with smtplib.SMTP_SSL("smtp.gmail.com", 465) as server:
        server.login(gmail_user, gmail_pass)
        server.sendmail(gmail_user, notify_email, msg.as_string())

    print(f"Email sent to {notify_email}")

#-- SLACK Functionality Added --

def send_slack(subject, body):
    webhook_url = os.environ["SLACK_WEBHOOK_URL"]
    # Format nicely for Slack
    slack_text = f"*{subject}*\n\n```{body}```"
    requests.post(webhook_url, json={"text": slack_text})
    print("Slack message sent.")

# ── Main ──────────────────────────────────────────────────────────────────────

ET = ZoneInfo("America/New_York")

def main():
    now = datetime.now(ET)

    # First run of the day gets full digest; subsequent runs only alert on new sprays
    today_str = now.strftime("%Y-%m-%d")
    seen_data_raw = json.loads(SEEN_FILE.read_text()) if SEEN_FILE.exists() else {}
    if isinstance(seen_data_raw, list):
        last_digest_date = ""
    else:
        last_digest_date = seen_data_raw.get("last_digest_date", "")
    morning_run = last_digest_date != today_str

    print(f"Fetching GPAS records at {now}...")
    records = fetch_records()
    print(f"  Fetched {len(records)} records total.")

    new_sprays, active_reis, upcoming_reis, scheduled = analyze_records(records, now)

    print(f"  New sprays for my blocks:   {len(new_sprays)}")
    print(f"  Active REIs for my blocks:  {len(active_reis)}")
    print(f"  Upcoming REIs (< {REI_WARN_HOURS}h):     {len(upcoming_reis)}")
    print(f"  Scheduled sprays:           {len(scheduled)}")
    print(f"  Morning run (full digest):  {morning_run}")

    body = build_email_body(now, new_sprays, active_reis, upcoming_reis, scheduled)

    # Morning run includes all alerts; daytime runs only fire on new sprays
    flags = []
    if new_sprays:
        flags.append(f"🆕 {len(new_sprays)} new spray(s)")
    if morning_run:
        if active_reis:
            flags.append(f"🚫 {len(active_reis)} ACTIVE REI")
        if upcoming_reis:
            flags.append(f"⏰ REI expiring soon")
        if scheduled:
            flags.append(f"📅 spray scheduled")

    if flags:
        subject = f"GPAS Alert: {' | '.join(flags)}"
    elif active_reis:
        subject = f"GPAS Daily — {len(active_reis)} active REI(s), no new sprays ({now.strftime('%b %d')})"
    elif scheduled:
        subject = f"GPAS Daily — spray(s) scheduled, no new activity ({now.strftime('%b %d')})"
    else:
        subject = f"GPAS Daily — all clear ({now.strftime('%b %d')})"
      
    print(f"\nSubject: {subject}")
    print("-" * 60)
    print(body)

    force_daily = os.environ.get("FORCE_DAILY", "false").lower() == "true"
    has_alerts = bool(new_sprays) if not morning_run else bool(new_sprays or active_reis or upcoming_reis or scheduled)

    if has_alerts or force_daily:
        save_seen(load_seen(), mark_digest_today=morning_run)
        send_email(subject, body)
        if os.environ.get("SLACK_WEBHOOK_URL") and os.environ.get("SLACK_ENABLED", "true") == "true":
            send_slack(subject, body)
    else:
        print("\nNo alerts and FORCE_DAILY not set — skipping email.")


if __name__ == "__main__":
    main()
