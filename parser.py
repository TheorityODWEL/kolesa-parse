import csv
import re
import time
from random import uniform
from typing import List, Dict, Optional, Tuple

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
from bs4 import BeautifulSoup


BASE_URL = "https://kolesa.kz"
URL = f"{BASE_URL}/cars/"

MAX_PAGES = 200          
STOP_AFTER_BLOCKS = 5     

HEADERS = {
    "accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8",
    "accept-language": "ru-RU,ru;q=0.9,en-US;q=0.7,en;q=0.6",
    "cache-control": "no-cache",
    "pragma": "no-cache",
    "referer": "https://kolesa.kz/",
    "user-agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/123.0 Safari/537.36"
    ),
}


# -----------------------------
# HTTP
# -----------------------------
def build_session() -> requests.Session:
    s = requests.Session()

    retry = Retry(
        total=6,
        connect=6,
        read=6,
        backoff_factor=1.0,
        status_forcelist=(429, 500, 502, 503, 504),
        allowed_methods=("GET",),
        respect_retry_after_header=True,
        raise_on_status=False,
    )
    adapter = HTTPAdapter(max_retries=retry, pool_connections=10, pool_maxsize=10)
    s.mount("https://", adapter)
    s.mount("http://", adapter)
    s.headers.update(HEADERS)
    return s


def fetch(session: requests.Session, url: str) -> Tuple[int, str, str]:
    r = session.get(url, timeout=(7, 25), allow_redirects=True)
    return r.status_code, r.url, r.text


def save_debug_html(page_no: int, status_code: int, final_url: str, html: str) -> str:
    fname = f"debug_page_{page_no}.html"
    with open(fname, "w", encoding="utf-8") as f:
        f.write(f"<!-- status_code={status_code} final_url={final_url} -->\n")
        f.write(html)
    return fname


# -----------------------------
# Page detection & pagination
# -----------------------------
def is_listings_page(html: str) -> bool:
    
    if "a-card__title" in html and "a-card__link" in html:
        return True

    if "a-elem" in html and ("a-el-info-title" in html or "a-el-info-price" in html):
        return True
    return False


def pages_count(html: str) -> int:
    soup = BeautifulSoup(html, "lxml")

    pager = soup.find("div", class_="pager") or soup.find("nav", class_="pager")
    if not pager:
        return 1

    nums = []
    for a in pager.find_all("a"):
        t = (a.get_text() or "").strip()
        if t.isdigit():
            nums.append(int(t))

    if not nums:
        return 1

    
    return min(max(nums), MAX_PAGES)


# -----------------------------
# Parsing
# -----------------------------
def _norm_price(text: str) -> Optional[int]:
    if not text:
        return None
    digits = re.sub(r"[^\d]", "", text)
    return int(digits) if digits else None


def parse_cards_new(soup: BeautifulSoup) -> List[Dict]:
   
    out = []
    cards = soup.select("div.a-card")
    for c in cards:
        title_a = c.select_one("h5.a-card__title a.a-card__link")
        if not title_a:
            continue

        name = title_a.get_text(strip=True)
        href = title_a.get("href")
        link = (BASE_URL + href) if href and href.startswith("/") else href

        # Цена может быть в разных местах; попробуем несколько вариантов
        price_node = c.select_one(".a-card__price, .price, .price-in-list .price")
        price_raw = price_node.get_text(" ", strip=True) if price_node else None
        price = _norm_price(price_raw) if price_raw else None

        # Город/описание тоже может быть по-разному
        desc_node = c.select_one(".a-card__description, .a-card__subtitle, .card__description")
        desc = desc_node.get_text(" ", strip=True) if desc_node else None

        out.append({
            "name": name,
            "price": price,
            "price_raw": price_raw,
            "desc": desc,
            "link": link,
        })
    return out


def parse_cards_old(soup: BeautifulSoup) -> List[Dict]:
    
    out = []
    blocks = soup.find_all("div", {"class": "a-elem"})
    for b in blocks:
        title_a = b.find("a", class_="a-el-info-title") or b.find("span", class_="a-el-info-title")
        name = title_a.get_text(strip=True) if title_a else None

        price_node = b.find("span", class_="a-el-info-price") or b.find("span", class_="price")
        price_raw = price_node.get_text(" ", strip=True) if price_node else None
        price = _norm_price(price_raw) if price_raw else None

        link = None
        if hasattr(title_a, "get") and title_a.get("href"):
            href = title_a.get("href")
            link = (BASE_URL + href) if href.startswith("/") else href

        desc_node = b.find("div", class_="a-el-info-description") or b.find("div", class_="a-search-description")
        desc = desc_node.get_text(" ", strip=True) if desc_node else None

        if name:
            out.append({
                "name": name,
                "price": price,
                "price_raw": price_raw,
                "desc": desc,
                "link": link,
            })
    return out


def parse_listings(html: str) -> List[Dict]:
    soup = BeautifulSoup(html, "lxml")

    data = []
    data.extend(parse_cards_new(soup))
    if not data:
        data.extend(parse_cards_old(soup))

    return data


# -----------------------------
# Output
# -----------------------------
def save_to_csv(rows: List[Dict], filename: str) -> None:
    fields = ["name", "price", "price_raw", "desc", "link"]
    with open(filename, "w", newline="", encoding="utf-8-sig") as f:
        w = csv.DictWriter(f, fieldnames=fields)
        w.writeheader()
        w.writerows(rows)


# -----------------------------
# Main
# -----------------------------
def main():
    session = build_session()

    status, final_url, first_html = fetch(session, URL)
    if not is_listings_page(first_html):
        fname = save_debug_html(1, status, final_url, first_html)
        print(f"[FATAL] Page 1 is not listings. status={status} final_url={final_url}")
        print(f"[DEBUG] Saved html to: {fname}")
        return

    last_page = pages_count(first_html)
    print(f"Total pages (capped): {last_page}")

    all_rows: List[Dict] = []
    all_rows.extend(parse_listings(first_html))

    consecutive_blocks = 0

    for i in range(2, last_page + 1):
        page_url = f"{URL}?page={i}"

        try:
            status, final_url, html = fetch(session, page_url)
        except requests.RequestException as e:
            print(f"[WARN] page={i} request failed: {e}")
            time.sleep(5)
            continue

        if not is_listings_page(html):
            consecutive_blocks += 1
            fname = save_debug_html(i, status, final_url, html)
            print(f"[WARN] page={i} NOT-LISTINGS (maybe blocked). status={status} final_url={final_url} saved={fname}")
            time.sleep(10 + uniform(2.0, 6.0))
            if consecutive_blocks >= STOP_AFTER_BLOCKS:
                print(f"[STOP] {consecutive_blocks} pages in a row are not listings. Stopping.")
                break
            continue

        consecutive_blocks = 0

        rows = parse_listings(html)
        all_rows.extend(rows)


        time.sleep(round(uniform(1.5, 4.0), 2))

        if i % 20 == 0:
            save_to_csv(all_rows, "cars_kolesa_partial.csv")
            print(f"[INFO] saved partial: {len(all_rows)} rows")

    save_to_csv(all_rows, "cars_kolesa.csv")
    print(f"Done. Saved rows: {len(all_rows)}")


if __name__ == "__main__":
    main()
