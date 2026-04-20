"""
Download the list of CDEs from the HEAL CDE repository website and write a CSV
compatible with heal_cde_repo_downloader.py (--heal-cde-csv flag).
"""
import csv
import collections
import re

import click
import logging
import requests
from bs4 import BeautifulSoup

REPO_PATH = (
    "/heal/heal-initiative-requirements/data-sharing-policy"
    "/common-data-elements-cdes-program/cdes-repository"
)

logging.basicConfig(level=logging.INFO)


def fetch_page(session, base_url, page):
    resp = session.get(base_url + REPO_PATH, params={"page": page})
    resp.raise_for_status()
    return BeautifulSoup(resp.text, "html.parser")


def get_last_page(soup):
    page_nums = []
    for a in soup.select("nav.usa-pagination a[href]"):
        m = re.search(r"[?&]page=(\d+)", a["href"])
        if m:
            page_nums.append(int(m.group(1)))
    return max(page_nums) if page_nums else 0


def parse_rows(soup, base_url):
    """Yield one dict per file found on the page."""
    for tr in soup.select("tr"):
        desc_td = tr.find("td", class_="views-field-body")
        files_td = tr.find("td", class_="views-field-field-cde-files")
        if not (desc_td and files_td):
            continue
        description = desc_td.get_text(strip=True)
        for a in files_td.find_all("a"):
            href = a.get("href", "")
            if not href:
                continue
            url = href if href.startswith("http") else base_url + href
            title = url.rstrip("/").split("/")[-1]
            yield {"Title": title, "Description": description, "File Language": "", "Link to File": url}


@click.command()
@click.argument("output", type=click.Path())
@click.option("--base-url", default="https://www.nih.gov", show_default=True,
              help="Base URL for the NIH website")
def get_heal_cde_list(output, base_url):
    """Download the HEAL CDE repository list and write a CSV file."""
    session = requests.Session()
    session.headers["User-Agent"] = "heal-cdes/1.0 (research)"

    logging.info("Fetching page 1 to determine total pages...")
    first_page = fetch_page(session, base_url, 0)
    last_page = get_last_page(first_page)
    total_pages = last_page + 1
    logging.info(f"Total pages: {total_pages}")

    rows = []
    ext_counts = collections.Counter()

    for page_num in range(total_pages):
        logging.info(f"Fetching page {page_num + 1}/{total_pages}...")
        soup = first_page if page_num == 0 else fetch_page(session, base_url, page_num)
        for row in parse_rows(soup, base_url):
            ext = "." + row["Title"].rsplit(".", 1)[-1].lower() if "." in row["Title"] else ""
            if ext not in {".xlsx", ".docx", ".pdf"}:
                logging.warning(f"Skipping file with unexpected extension: {row['Link to File']}")
                continue
            ext_counts[ext] += 1
            rows.append(row)

    with open(output, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=["Title", "Description", "File Language", "Link to File"])
        writer.writeheader()
        writer.writerows(rows)

    total = len(rows)
    logging.info(
        f"Wrote {total} files to {output} "
        f"({ext_counts.get('.xlsx', 0)} xlsx, "
        f"{ext_counts.get('.docx', 0)} docx, "
        f"{ext_counts.get('.pdf', 0)} pdf)"
    )


if __name__ == "__main__":
    get_heal_cde_list()
