from playwright.sync_api import sync_playwright
import pandas as pd
import os

TARGET_URL = "https://hpa.co.zw/registered-facilities/"
os.makedirs("data", exist_ok=True)

def scrape_all_facilities():
    facilities = []
    seen = set()
    page_number = 1

    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True)
        page = browser.new_page()

        page.goto(TARGET_URL, wait_until="domcontentloaded", timeout=120000)
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
            page.wait_for_selector("table#tablepress-1 tbody tr")
            page_number += 1

        browser.close()

    return facilities


if __name__ == "__main__":
    data = scrape_all_facilities()
    df = pd.DataFrame(data)
    df.to_csv("data/facilities_raw1.csv", index=False)

    print(f"✅ Scraped {len(df)} unique facilities")