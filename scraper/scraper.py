from playwright.sync_api import sync_playwright
import pandas as pd
import os
import time
import random

TARGET_URL = "https://hpa.co.zw/registered-facilities/"
os.makedirs("data", exist_ok=True)

# Stealth user agents
USER_AGENTS = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.114 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:89.0) Gecko/20100101 Firefox/89.0",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/14.1.1 Safari/605.1.15",
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.101 Safari/537.36"
]

def scrape_all_facilities():
    facilities = []
    seen = set()
    page_number = 1

    with sync_playwright() as p:
        # Launch browser with stealth options
        browser = p.chromium.launch(
            headless=True,
            args=[
                '--no-sandbox',
                '--disable-setuid-sandbox',
                '--disable-dev-shm-usage',
                '--disable-accelerated-2d-canvas',
                '--no-first-run',
                '--no-zygote',
                '--single-process',  # <- this one doesn't work in Windows
                '--disable-gpu'
            ]
        )

        # Create context with random user agent
        user_agent = random.choice(USER_AGENTS)
        context = browser.new_context(
            user_agent=user_agent,
            viewport={'width': 1280, 'height': 720},
            extra_http_headers={
                'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
                'Accept-Language': 'en-US,en;q=0.5',
                'Accept-Encoding': 'gzip, deflate',
                'DNT': '1',
                'Connection': 'keep-alive',
                'Upgrade-Insecure-Requests': '1',
            }
        )
        page = context.new_page()

        page.goto(TARGET_URL, wait_until="domcontentloaded", timeout=120000)
        # Random delay after loading
        time.sleep(random.uniform(2, 5))
        page.wait_for_selector("table#tablepress-1", timeout=120000)

        while True:
            print(f"📄 Scraping page {page_number}...")

            # ⚡ Extract ALL rows at once (FAST)
            rows = page.locator("table#tablepress-1 tbody tr").evaluate_all("""
                rows => rows.map(row => {
                    const cells = row.querySelectorAll("td");
                    return {
                        name: cells[0]?.innerText.trim(),
                        address: cells[1]?.innerText.trim(),
                        city: cells[2]?.innerText.trim()
                    };
                })
            """)

            for r in rows:
                key = f"{r['name']}|{r['address']}|{r['city']}"
                if key in seen:
                    continue

                seen.add(key)
                facilities.append({
                    "Facility Name": r["name"],
                    "Physical Address": r["address"],
                    "City": r["city"]
                })

            # 👉 Click Next if enabled
            next_btn = page.locator("#tablepress-1_next")

            if "disabled" in (next_btn.get_attribute("class") or ""):
                break

            next_btn.click()
            # Random delay between page clicks
            time.sleep(random.uniform(3, 7))
            page.wait_for_selector("table#tablepress-1 tbody tr")
            page_number += 1

        browser.close()

    return facilities


if __name__ == "__main__":
    data = scrape_all_facilities()
    df = pd.DataFrame(data)
    df.to_csv("data/facilities_raw1.csv", index=False)

    print(f"✅ Scraped {len(df)} unique facilities")