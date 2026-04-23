"""
Backstop for get_heal_cde_list.py: loads the CSV it produced, then walks every
node in the HEAL CDE supplemental taxonomy listing. Any node whose XLSX file is
not already in the input CSV is added (all its files) to the output CSV.

Output CSV has the same columns as get_heal_cde_list.py so it can feed directly
into heal_cde_repo_downloader.py.
"""
import csv
import re
import time

import click
import logging
import requests
from bs4 import BeautifulSoup

logging.basicConfig(level=logging.INFO)

BASE_URL = "https://www.nih.gov"
FIELDNAMES = ["Title", "Description", "File Language", "Link to File",
              "Core or Supplemental", "CDE Topics"]


def fetch_soup(session, url):
    resp = session.get(url)
    resp.raise_for_status()
    return BeautifulSoup(resp.text, "html.parser")


def next_page_url(soup):
    a = soup.select_one("a[rel='next']")
    if not a:
        return None
    href = a["href"]
    return href if href.startswith("http") else BASE_URL + href


def parse_listing_page(soup):
    """Yield (title, node_path) for each CDE row on a listing page."""
    for div in soup.select("div.views-row"):
        a = div.find("a", href=re.compile(r"^/node/\d+$"))
        if not a:
            continue
        h3 = a.find("h3", class_="teaser__heading")
        title = h3.get_text(strip=True) if h3 else a.get_text(strip=True)
        yield title, a["href"]


def parse_node_page(soup, default_category="Supplemental"):
    """Return (description, category, topic, [(filename, language, url)])."""
    body_div = soup.find("div", class_="field--name-body")
    description = body_div.get_text(" ", strip=True) if body_div else ""

    cat_div = soup.find("div", class_="field--name-field-cde-category")
    category = ", ".join(
        item.get_text(strip=True)
        for item in cat_div.select("div.field__item")
    ) if cat_div else default_category

    topic_div = soup.find("div", class_="field--name-field-heal-research-topic")
    topic = ", ".join(
        item.get_text(strip=True)
        for item in topic_div.select("div.field__item")
    ) if topic_div else ""

    files = []
    for article in soup.select("article.media--type-cde-files"):
        a = article.find("a", href=True)
        if not a:
            continue
        href = a["href"]
        url = href if href.startswith("http") else BASE_URL + href
        filename = href.rstrip("/").split("/")[-1]
        lang_div = article.find("div", class_="field--name-field-cde-file-language")
        language = lang_div.get_text(strip=True) if lang_div else ""
        files.append((filename, language, url))

    return description, category, topic, files


@click.command()
@click.argument("output", type=click.Path())
@click.option("--input-csv", required=True, type=click.Path(exists=True),
              help="CSV produced by get_heal_cde_list.py (pass-through base)")
@click.option("--taxonomy-url", default=f"{BASE_URL}/taxonomy/term/1441",
              show_default=True, help="Taxonomy listing URL")
@click.option("--delay", default=0.3, show_default=True,
              help="Seconds to wait between node-page requests")
@click.option("--default-category", default="Supplemental", show_default=True,
              help="Fallback category when a node page has no category field")
def get_heal_cde_taxonomy(output, input_csv, taxonomy_url, delay, default_category):
    """Backstop: extend get_heal_cde_list.py output with taxonomy-only CDEs."""
    session = requests.Session()
    session.headers["User-Agent"] = "heal-cdes/1.0 (research)"

    # Load the input CSV; build a set of existing XLSX titles for lookup.
    with open(input_csv, newline="", encoding="utf-8") as f:
        input_rows = list(csv.DictReader(f))
    existing_xlsx = {
        r["Title"] for r in input_rows
        if r["Title"].lower().endswith(".xlsx")
    }
    logging.info(f"Loaded {len(input_rows)} rows from input CSV "
                 f"({len(existing_xlsx)} unique XLSX titles).")

    # Walk every taxonomy listing page.
    nodes = []
    url = taxonomy_url
    page_num = 0
    while url:
        logging.info(f"Listing page {page_num} ({url})...")
        soup = fetch_soup(session, url)
        for title, path in parse_listing_page(soup):
            nodes.append((title, path))
        url = next_page_url(soup)
        page_num += 1
    logging.info(f"Found {len(nodes)} taxonomy nodes; checking each...")

    # For each node, decide whether it needs to be added.
    new_rows = []
    covered = 0
    added_nodes = 0
    for i, (node_title, path) in enumerate(nodes, 1):
        node_url = BASE_URL + path
        try:
            node_soup = fetch_soup(session, node_url)
            description, category, topic, files = parse_node_page(node_soup, default_category)
        except Exception as e:
            logging.warning(f"[{i}/{len(nodes)}] Failed to fetch {node_url}: {e}")
            if delay:
                time.sleep(delay)
            continue

        xlsx_files = [fn for fn, _, _ in files if fn.lower().endswith(".xlsx")]
        if any(fn in existing_xlsx for fn in xlsx_files):
            covered += 1
            logging.info(f"[{i}/{len(nodes)}] Already covered: {node_title}")
        else:
            added_nodes += 1
            logging.info(f"[{i}/{len(nodes)}] Adding new node: {node_title}")
            for filename, language, file_url in files:
                ext = "." + filename.rsplit(".", 1)[-1].lower() if "." in filename else ""
                if ext not in {".xlsx", ".docx", ".pdf"}:
                    continue
                new_rows.append({
                    "Title": filename,
                    "Description": description,
                    "File Language": language,
                    "Link to File": file_url,
                    "Core or Supplemental": category,
                    "CDE Topics": topic,
                })

        if delay and i < len(nodes):
            time.sleep(delay)

    with open(output, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=FIELDNAMES)
        writer.writeheader()
        writer.writerows(input_rows)
        writer.writerows(new_rows)

    logging.info(
        f"Done: {covered} nodes already covered, {added_nodes} nodes added "
        f"({len(new_rows)} new file rows). "
        f"Output: {len(input_rows) + len(new_rows)} total rows → {output}"
    )


if __name__ == "__main__":
    get_heal_cde_taxonomy()
