"""
debug_upstox_login.py — Visual step-by-step Upstox login debugger

Runs the login with screenshots at every step so we can see exactly
where it gets stuck. Screenshots saved to logs/step_*.png.

Run:  python debug_upstox_login.py
"""

import os
import re
import time
from pathlib import Path

import pyotp
from dotenv import load_dotenv
from playwright.sync_api import sync_playwright

load_dotenv()

BASE_DIR = Path(__file__).parent
LOG_DIR = BASE_DIR / "logs"
LOG_DIR.mkdir(exist_ok=True)

client_id = os.getenv("UPSTOX_CLIENT_ID", "")
client_secret = os.getenv("UPSTOX_CLIENT_SECRET", "")
redirect_uri = os.getenv("UPSTOX_REDIRECT_URI", "http://127.0.0.1")
mobile = os.getenv("UPSTOX_MOBILE", "")
pin = os.getenv("UPSTOX_PIN", "")
totp_secret = os.getenv("UPSTOX_TOTP_SECRET", "")

auth_url = (
    f"https://api.upstox.com/v2/login/authorization/dialog"
    f"?response_type=code&client_id={client_id}&redirect_uri={redirect_uri}"
)


def snap(page, name):
    path = LOG_DIR / f"{name}.png"
    page.screenshot(path=str(path), full_page=True)
    print(f"  Screenshot: {path}")


print("\n=== Upstox Login Debugger ===")
print(f"Auth URL: {auth_url[:80]}...")
print(f"Mobile:   {mobile[:4]}***")
print(f"TOTP now: {pyotp.TOTP(totp_secret).now()}")
print()

with sync_playwright() as p:
    browser = p.chromium.launch(
        headless=True,
        args=[
            "--no-sandbox",
            "--disable-blink-features=AutomationControlled",
            "--disable-dev-shm-usage",
        ]
    )

    try:
        from playwright_stealth import stealth_sync
        stealth_available = True
        print("playwright-stealth: available")
    except ImportError:
        stealth_available = False
        print("playwright-stealth: not installed")

    context = browser.new_context(
        user_agent=(
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
            "AppleWebKit/537.36 (KHTML, like Gecko) "
            "Chrome/124.0.0.0 Safari/537.36"
        ),
        viewport={"width": 1280, "height": 800},
    )
    page = context.new_page()

    if stealth_available:
        stealth_sync(page)
        print("Stealth mode: enabled")

    # Step 1 — Load auth URL
    print("\n[1] Loading Upstox auth page...")
    page.goto(auth_url, wait_until="networkidle", timeout=30000)
    snap(page, "step1_auth_page")
    print(f"    URL: {page.url}")
    print(f"    Title: {page.title()}")
    page.wait_for_timeout(2000)

    # Step 2 — Mobile number
    print("\n[2] Entering mobile number...")
    try:
        # Try various selectors
        for selector in [
            "input[id*='mobileNum']",
            "input[name*='mobile']",
            "input[type='tel']",
            "input[placeholder*='Mobile']",
            "input[placeholder*='mobile']",
            "input[type='text']",
        ]:
            try:
                el = page.wait_for_selector(selector, timeout=3000, state="visible")
                if el:
                    el.fill(mobile)
                    print(f"    Filled mobile using selector: {selector}")
                    break
            except Exception:
                continue

        snap(page, "step2_mobile_filled")

        # Click get OTP / continue
        for btn_text in ["Get OTP", "Continue", "Next", "Send OTP"]:
            try:
                page.click(f"button:has-text('{btn_text}')", timeout=3000)
                print(f"    Clicked: '{btn_text}'")
                break
            except Exception:
                continue

        page.wait_for_timeout(3000)
        snap(page, "step2_after_mobile_submit")
        print(f"    URL after mobile: {page.url}")

    except Exception as e:
        print(f"    ERROR: {e}")
        snap(page, "step2_error")

    # Step 3 — PIN
    print("\n[3] Entering PIN...")
    try:
        page.wait_for_timeout(2000)
        snap(page, "step3_before_pin")

        # Try individual digit boxes first (OTP-style input)
        pin_boxes = page.query_selector_all("input[maxlength='1'][type='password'], input[maxlength='1'][type='text']")
        if len(pin_boxes) >= len(pin):
            print(f"    Found {len(pin_boxes)} individual PIN digit boxes")
            for i, digit in enumerate(pin):
                pin_boxes[i].click()
                pin_boxes[i].type(digit)
                page.wait_for_timeout(150)
        else:
            # Try single field
            for selector in [
                "input[type='password']",
                "input[placeholder*='PIN']",
                "input[placeholder*='pin']",
            ]:
                try:
                    el = page.wait_for_selector(selector, timeout=3000, state="visible")
                    if el:
                        el.fill(pin)
                        print(f"    Filled PIN using: {selector}")
                        break
                except Exception:
                    continue

        snap(page, "step3_pin_filled")

        for btn_text in ["Continue", "Next", "Login", "Submit"]:
            try:
                page.click(f"button:has-text('{btn_text}')", timeout=3000)
                print(f"    Clicked: '{btn_text}'")
                break
            except Exception:
                continue

        page.wait_for_timeout(3000)
        snap(page, "step3_after_pin_submit")
        print(f"    URL after PIN: {page.url}")

    except Exception as e:
        print(f"    ERROR: {e}")
        snap(page, "step3_error")

    # Step 4 — TOTP
    print("\n[4] Entering TOTP...")
    try:
        totp = pyotp.TOTP(totp_secret)
        otp_code = totp.now()
        print(f"    TOTP code: {otp_code} (verify this matches your app!)")

        page.wait_for_timeout(2000)
        snap(page, "step4_before_totp")

        otp_boxes = page.query_selector_all("input[maxlength='1']")
        if len(otp_boxes) >= 6:
            print(f"    Found {len(otp_boxes)} individual OTP digit boxes")
            for i, digit in enumerate(otp_code):
                otp_boxes[i].click()
                otp_boxes[i].type(digit)
                page.wait_for_timeout(150)
        else:
            for selector in [
                "input[maxlength='6']",
                "input[placeholder*='OTP']",
                "input[placeholder*='TOTP']",
                "input[placeholder*='otp']",
                "input[type='text']",
            ]:
                try:
                    el = page.wait_for_selector(selector, timeout=3000, state="visible")
                    if el:
                        el.fill(otp_code)
                        print(f"    Filled TOTP using: {selector}")
                        break
                except Exception:
                    continue

        snap(page, "step4_totp_filled")

        for btn_text in ["Continue", "Verify", "Submit", "Next"]:
            try:
                page.click(f"button:has-text('{btn_text}')", timeout=3000)
                print(f"    Clicked: '{btn_text}'")
                break
            except Exception:
                continue

        # Wait for redirect
        page.wait_for_timeout(5000)
        snap(page, "step4_after_totp_submit")
        final_url = page.url
        print(f"    URL after TOTP: {final_url}")

        # Check for auth code
        match = re.search(r"[?&]code=([^&]+)", final_url)
        if match:
            print(f"\n=== SUCCESS! Auth code: {match.group(1)[:20]}... ===")
        else:
            print(f"\n=== No auth code in URL. Final URL: {final_url} ===")
            print("    Check steps4_after_totp_submit.png to see what the page shows")

    except Exception as e:
        print(f"    ERROR: {e}")
        snap(page, "step4_error")

    # List all page inputs visible
    print("\n[Debug] All visible inputs on final page:")
    inputs = page.query_selector_all("input:visible")
    for inp in inputs:
        attrs = {a: inp.get_attribute(a) for a in ["type", "name", "id", "placeholder", "maxlength"]}
        print(f"    {attrs}")

    browser.close()

print("\n=== Screenshots saved to logs/ — review them to see exactly what happened ===")
