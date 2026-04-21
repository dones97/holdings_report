"""
get_token.py — Interactive Upstox Setup Helper

Run this ONCE to:
  1. Walk through getting your TOTP secret from Upstox
  2. Validate your credentials
  3. Confirm automated login works

After this, you never need to touch tokens again.

Usage:
  python scripts/get_token.py
"""

import os
import re
import sys
import webbrowser
from pathlib import Path

# Add parent to path
sys.path.insert(0, str(Path(__file__).parent.parent))

from dotenv import load_dotenv, set_key
import httpx
import pyotp

ENV_PATH = Path(__file__).parent.parent / ".env"
load_dotenv(ENV_PATH)


def print_banner():
    print()
    print("=" * 60)
    print("  Holdings Report Agent — Upstox Setup")
    print("=" * 60)
    print()


def print_step(n, title):
    print()
    print(f"[Step {n}] {title}")
    print("-" * 50)


def validate_env():
    """Check if all required variables are in .env."""
    required = {
        "UPSTOX_CLIENT_ID": os.getenv("UPSTOX_CLIENT_ID"),
        "UPSTOX_CLIENT_SECRET": os.getenv("UPSTOX_CLIENT_SECRET"),
    }
    missing = [k for k, v in required.items() if not v or v in ("FILL_IN", "your_api_key_here")]
    if missing:
        print(f"  ✗ Missing in .env: {', '.join(missing)}")
        print()
        print("  Edit your .env file and add:")
        for k in missing:
            print(f"    {k}=<your value>")
        print()
        print("  Get these from: https://account.upstox.com/developer/apps")
        return False
    print(f"  ✓ UPSTOX_CLIENT_ID: {required['UPSTOX_CLIENT_ID'][:12]}...")
    return True


def explain_totp():
    print("""
  Your TOTP secret is the base32 seed that your authenticator app uses
  to generate 6-digit codes. To get it:

  Option A — During 2FA setup (recommended if setting up fresh):
    1. Go to Upstox app → Profile → Security → Two-Factor Authentication
    2. When shown the QR code, look for "Can't scan? Enter this code manually"
    3. That text code (looks like: JBSWY3DPEHPK3PXP) is your TOTP secret

  Option B — If already set up with Google Authenticator:
    1. Open Google Authenticator on your phone
    2. Long-press the Upstox entry → tap the pencil/edit icon
    3. You may see the key displayed — note it down

  Option C — Export from Google Authenticator:
    1. Google Authenticator → Menu → Transfer Accounts → Export
    2. Scan with a decoder app to extract the base32 secret

  The secret looks like: JBSWY3DPEHPK3PXP (16-32 uppercase letters + digits)
    """)


def get_totp_secret() -> str:
    while True:
        secret = input("  Enter your Upstox TOTP secret (or press Enter to skip): ").strip()
        if not secret:
            print("  Skipping TOTP setup. You can add UPSTOX_TOTP_SECRET to .env manually.")
            return ""

        # Validate it looks like a base32 secret
        secret_clean = secret.upper().replace(" ", "").replace("-", "")
        if not re.match(r'^[A-Z2-7]+=*$', secret_clean):
            print(f"  ✗ That doesn't look like a valid base32 secret. Try again.")
            continue

        try:
            totp = pyotp.TOTP(secret_clean)
            code = totp.now()
            print(f"  ✓ TOTP secret valid. Current code: {code}")
            return secret_clean
        except Exception as e:
            print(f"  ✗ Invalid TOTP secret: {e}. Try again.")


def test_login(mobile: str, pin: str, totp_secret: str) -> bool:
    """Attempt a real token fetch to validate all credentials."""
    client_id = os.getenv("UPSTOX_CLIENT_ID")
    client_secret = os.getenv("UPSTOX_CLIENT_SECRET")
    redirect_uri = os.getenv("UPSTOX_REDIRECT_URI", "http://127.0.0.1")

    print()
    print("  Testing automated login...")

    try:
        headers = {"accept": "application/json", "content-type": "application/json"}
        with httpx.Client(timeout=20.0) as session:
            # Step 1
            r1 = session.post(
                "https://api.upstox.com/v3/login/authorization/users",
                json={"mobile_number": mobile, "pin": pin},
                headers=headers,
            )
            r1.raise_for_status()
            d1 = r1.json()
            if d1.get("status") != "success":
                print(f"  ✗ Login failed: {d1}")
                return False

            print("  ✓ Mobile + PIN accepted")
            session_token = d1["data"]["token"]

            # Step 2
            totp = pyotp.TOTP(totp_secret)
            r2 = session.post(
                "https://api.upstox.com/v3/login/authorization/otp/verify",
                json={"otp": totp.now(), "is_push_notification": False, "token": session_token},
                headers=headers,
            )
            r2.raise_for_status()
            d2 = r2.json()
            if d2.get("status") != "success":
                print(f"  ✗ TOTP verification failed: {d2}")
                return False

            print("  ✓ TOTP verified")
            auth_token = d2["data"]["token"]

            # Step 3
            r3 = session.get(
                "https://api.upstox.com/v3/login/authorization/dialog",
                params={
                    "client_id": client_id,
                    "redirect_uri": redirect_uri,
                    "response_type": "code",
                    "token": auth_token,
                },
                follow_redirects=False,
            )
            location = r3.headers.get("location", "")
            match = re.search(r"[?&]code=([^&]+)", location)
            if not match:
                print(f"  ✗ Could not get auth code. Location: {location[:100]}")
                return False

            auth_code = match.group(1)
            print("  ✓ Auth code obtained")

            # Step 4
            r4 = session.post(
                "https://api.upstox.com/v2/login/authorization/token",
                data={
                    "code": auth_code,
                    "client_id": client_id,
                    "client_secret": client_secret,
                    "redirect_uri": redirect_uri,
                    "grant_type": "authorization_code",
                },
                headers={"accept": "application/json"},
            )
            r4.raise_for_status()
            token_data = r4.json()
            access_token = token_data.get("access_token", "")

            if not access_token:
                print(f"  ✗ No access token in response: {token_data}")
                return False

            print(f"  ✓ Access token obtained ({len(access_token)} chars)")
            print()
            print("  ✅ Automated login WORKS! Your pipeline will run without any manual steps.")
            return True

    except httpx.HTTPStatusError as e:
        print(f"  ✗ HTTP error {e.response.status_code}: {e.response.text[:200]}")
        return False
    except Exception as e:
        print(f"  ✗ Error: {e}")
        return False


def save_to_env(key: str, value: str):
    """Save a key=value to .env file."""
    set_key(str(ENV_PATH), key, value)
    print(f"  ✓ Saved {key} to .env")


def main():
    print_banner()

    # ── Step 1: Validate existing .env ───────────────────────────────────────
    print_step(1, "Checking existing .env credentials")
    if not validate_env():
        print("\n  Please update .env and re-run this script.")
        sys.exit(1)

    # ── Step 2: Mobile + PIN ──────────────────────────────────────────────────
    print_step(2, "Upstox Login Credentials")
    print("""
  The pipeline needs your Upstox mobile number and PIN to log in
  automatically every Sunday. These are stored in your local .env file
  and never sent anywhere except Upstox's own API.
    """)

    mobile = os.getenv("UPSTOX_MOBILE", "")
    if not mobile or mobile == "FILL_IN":
        mobile = input("  Enter your Upstox registered mobile (digits only, no +91): ").strip()
        if mobile:
            save_to_env("UPSTOX_MOBILE", mobile)

    pin = os.getenv("UPSTOX_PIN", "")
    if not pin or pin == "FILL_IN":
        pin = input("  Enter your Upstox PIN (4-6 digit trading PIN): ").strip()
        if pin:
            save_to_env("UPSTOX_PIN", pin)

    # ── Step 3: TOTP Secret ───────────────────────────────────────────────────
    print_step(3, "TOTP Secret (for automatic 2FA)")
    explain_totp()

    totp_secret = os.getenv("UPSTOX_TOTP_SECRET", "")
    if not totp_secret or totp_secret == "FILL_IN":
        totp_secret = get_totp_secret()
        if totp_secret:
            save_to_env("UPSTOX_TOTP_SECRET", totp_secret)

    # ── Step 4: Test Login ────────────────────────────────────────────────────
    if mobile and pin and totp_secret:
        print_step(4, "Testing automated login")
        success = test_login(mobile, pin, totp_secret)
        if not success:
            print("\n  ⚠️  Login test failed. Double-check your credentials in .env and retry.")
            sys.exit(1)
    else:
        print("\n  ⚠️  Skipping login test — some credentials missing.")
        print("      Edit .env manually and re-run this script.")

    # ── Done ──────────────────────────────────────────────────────────────────
    print()
    print("=" * 60)
    print("  ✅ Setup complete!")
    print()
    print("  Next steps:")
    print("  1. Add remaining keys to .env:")
    print("       GOOGLE_API_KEY=...")
    print("       BRAVE_SEARCH_API_KEY=...")
    print("       GMAIL_APP_PASSWORD=...")
    print()
    print("  2. Run a test: python scripts/test_run.py")
    print("  3. Set up schedule: powershell scripts/schedule_windows.ps1")
    print("=" * 60)
    print()


if __name__ == "__main__":
    main()
