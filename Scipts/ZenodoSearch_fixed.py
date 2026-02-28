#-------------------------------------------------------------------------------------------------- 
# Web crawler to retrieve datasets from Zenodo API.
#
# Created by Gerasimos Antzoulatos (CERTH) in the content of EU CSA Ideation project.
#
from datetime import datetime, timedelta
import requests
from tenacity import retry, stop_after_attempt, wait_exponential, retry_if_exception_type
import pandas as pd
import re
import html
import os
import time
from urllib.parse import quote_plus

# Read your Zenodo personal access token from an environment variable.
# # Example (bash): export ZENODO_TOKEN="..."

ACCESS_TOKEN = os.getenv("ZENODO_TOKEN", "").strip()
ZENODO_API_URL = "https://zenodo.org/api/records"
ZENODO_SEARCH_URL = "https://zenodo.org/search"  # Base URL for Zenodo search


# ----------------------------------------------------
# HTML cleaning helper
# ----------------------------------------------------
def clean_html(text):
    """Remove HTML tags and normalize whitespace."""
    if not isinstance(text, str):
        return ""
    # Decode HTML entities (&nbsp;, &amp;, etc.)
    text = html.unescape(text)
    # Remove tags
    text = re.sub(r"<[^>]+>", " ", text)
    # Collapse whitespace
    text = re.sub(r"\s+", " ", text).strip()
    return text


# ----------------------------------------------------
# Search phrases
# ----------------------------------------------------
search_phrases = ["river", "reservoir", "aquifers", "underground water", "lake", "lagoon", "ocean", "coastal", "ice", "sea", "waste water", "snow", "groundwater",
    "flood", "flash floods", "drought", "flow", "marine biodiversity", "leakage", "oil spill", "water quality", "irrigation",
    "water transport", "glacier", "water distribution", "rainfall", "water supply", "water contamination", "water treatment",  
    "water grid operation", "rain water", "precipitation", "water consumption", "water demand", "water conservation", 
    "hydrology", "natural hazard","snow melt", "extreme weather"]

# ----------------------------------------------------
# Check the validity of the date string 
# ----------------------------------------------------
def is_valid_date(date_str):
    """Check if date_str is a valid ISO date (YYYY-MM-DD)."""
    if not isinstance(date_str, str):
        return False
    try:
        datetime.strptime(date_str, "%Y-%m-%d")
        return True
    except ValueError:
        return False

def get_search_url(query):
    """Generate a Zenodo search URL (website) for datasets.

    Note: This is just for convenience/traceability. The API call is independent.
    """
    return f"{ZENODO_SEARCH_URL}?q={quote_plus(str(query))}&type=dataset"

#--------------------------------------------------------
# Extract publication year from date
#--------------------------------------------------------
def extract_publication_year(date_str):
    """Extract the YYYY year from an ISO publication date."""
    try:
        return datetime.strptime(date_str, "%Y-%m-%d").year
    except Exception:
        return None

# -----------------------------------------------------------------------------------------------------------
# Search Zenodo API
# -----------------------------------------------------------------------------------------------------------
# Setup: 5 attempts, starting with a 2-second delay, doubling the time.
@retry(
    stop = stop_after_attempt(5), 
    wait = wait_exponential(multiplier=1, min=2, max=10),
    retry = retry_if_exception_type((requests.exceptions.HTTPError, requests.exceptions.Timeout, requests.exceptions.ConnectionError, requests.exceptions.RequestException))
)

def search_zenodo(query, sort_by="mostrecent", years_filter=7):
    """Fetch datasets from Zenodo based on search query and filters.

    Notes:
    - years_filter is applied client-side against metadata.publication_date (ISO YYYY-MM-DD).
    - The 'type' parameter is kept as 'dataset' to match your intent; if Zenodo changes this,
      adjust here or move it into the 'q' field.
    """
    PAGE_SIZE = 100
    RESULTS_LIMIT = 1000
    min_date = (datetime.today() - timedelta(days=years_filter * 365)).strftime("%Y-%m-%d")

    params = {
        "q": query,
        "size": PAGE_SIZE,
        "type": "dataset",
        "sort": sort_by,
        "page": 1,
        "all_versions": True,
    }

    headers = {}
    if ACCESS_TOKEN:
        headers["Authorization"] = f"Bearer {ACCESS_TOKEN}"
    else:
        # Public records can still be queried without a token.
        # (Leaving this as a print so it's visible when you run the script.)
        print("⚠ No ZENODO_TOKEN found in environment; continuing without Authorization header.")

    all_datasets = []
    search_url = get_search_url(query)
    print(f"\n🔎 Searching Zenodo for: '{query}' → {search_url} (Sorted by {sort_by}, since {min_date})")

    while len(all_datasets) < RESULTS_LIMIT:
        response = requests.get(
            ZENODO_API_URL,
            params=params,
            headers=headers or None,
            timeout=(10, 30),  # (connect, read) timeouts
        )

        # Handle rate limiting politely (Zenodo may return 429).
        if response.status_code == 429:
            retry_after = response.headers.get("Retry-After")
            if retry_after and retry_after.isdigit():
                time.sleep(int(retry_after))
            # Raise to trigger tenacity retry.
            response.raise_for_status()

        # For all other non-2xx, raise so tenacity can retry (for 5xx etc.)
        response.raise_for_status()

        json_resp = response.json()
        results = json_resp.get("hits", {}).get("hits", [])
        print(f" Found {len(results)} results on page {params['page']}.")

        if not results:
            print(f"⚠ No results found for '{query}'")
            break

        # ✅ Filter by valid and recent publication dates
        filtered_results = []
        for r in results:
            pub_date = r.get("metadata", {}).get("publication_date", "")
            if not is_valid_date(pub_date):
                continue  # skip invalid or missing dates
            if pub_date >= min_date:
                filtered_results.append(r)

        all_datasets.extend(filtered_results[: RESULTS_LIMIT - len(all_datasets)])
        print(f"📄 Query '{query}': {len(filtered_results)} results (Total kept: {len(all_datasets)})")

        # Stop if there is no next page
        if "next" not in (json_resp.get("links") or {}):
            break

        params["page"] += 1

    return all_datasets, search_url


def extract_metadata(datasets, search_phrase, search_url):
    """Extract metadata from Zenodo datasets."""
    extracted_data = []
    for dataset in datasets:
        metadata = dataset.get("metadata", {})
        stats = dataset.get("stats", {})

        # Files are typically at the top level in /api/records responses.
        files = dataset.get("files") or metadata.get("files") or []

        record_id = dataset.get("id", "Unknown")
        dataset_html_link = dataset.get("links", {}).get("self_html", "https://zenodo.org")
        dataset_metadata_api = f"{ZENODO_API_URL}/{record_id}"

        # Clean description (remove HTML)
        description_raw = metadata.get("description", "No Description")
        description_clean = clean_html(description_raw)  

        pub_date = metadata.get("publication_date", "")
        pub_year = extract_publication_year(pub_date)

        extracted_data.append(
            {
                "Search Phrase": search_phrase,
                "Search Query URL": search_url,
                "Dataset Page": dataset_html_link,
                "Metadata API URL": dataset_metadata_api,
                "Title": metadata.get("title", "No Title"),
                "DOI": metadata.get("doi", "No DOI"),
                "Description": description_clean,
                "Creators": ", ".join(
                    [c.get("name", "Unknown") for c in metadata.get("creators", [])]
                ),
                #"Publication Date": metadata.get("publication_date", "Unknown Date"),
                "Publication Date": pub_date,
                "Publication Year": pub_year if pub_year else "Unknown",
                "Access": metadata.get("access_right", "Unknown Access"),
                "Keywords": ", ".join(metadata.get("keywords", []))
                if metadata.get("keywords")
                else "No Keywords",
                "License": metadata.get("license", {}).get("id", "No License"),
                "Version": metadata.get("version", "Unknown Version"),
                "Number of Files": len(files),
                "File Names": ", ".join(
                    [f.get("key") or f.get("filename") or "Unknown" for f in files]
                ),
                "File Sizes (bytes)": ", ".join(
                    [str(f.get("size", "Unknown")) for f in files]
                ),
                "File Links": ", ".join(
                    [ (f.get("links", {}) or {}).get("self") or (f.get("links", {}) or {}).get("download") or "Unknown" for f in files ]
                ),
                "Views": stats.get("views", 0),
                "Downloads": stats.get("downloads", 0),
                "Related Identifiers": ", ".join(
                    [r.get("identifier", "Unknown") for r in metadata.get("related_identifiers", [])]
                ),
                "Subjects": ", ".join(
                    [s.get("term", "Unknown") for s in metadata.get("subjects", [])]
                ),
                "Communities": ", ".join(
                    [c.get("title", "Unknown") for c in metadata.get("communities", [])]
                ),
                "Grants": ", ".join(
                    [g.get("id", "Unknown") for g in metadata.get("grants", [])]
                )
            }
        )

    return extracted_data


def search_and_save_results(search_phrases, sort_by="mostrecent", years_filter=5):
    """Search Zenodo and save results to CSV."""
    all_results = []
    for phrase in search_phrases:
        print(f"\n🟢 Processing search phrase: '{phrase}'...")

        try:
            datasets, search_url = search_zenodo(phrase, sort_by, years_filter)
            metadata_list = extract_metadata(datasets, phrase, search_url)
            all_results.extend(metadata_list)
        except Exception as e:
            print(f"Error processing phrase '{phrase}': {e}")

    if not all_results:
        print("\n⚠ No results collected, CSV will not be created.")
        return

    results_df = pd.DataFrame(all_results)

    # --- Add OpenKIWAS_ID (opendata-<search_phrase>-0001, 0002, ...)
    results_df["OpenKIWAS_ID"] = (
        "opendata-"
        + results_df["Search Phrase"].astype(str).str.replace(" ", "_", regex=False)
        + "-"
        + (results_df.groupby("Search Phrase").cumcount() + 1).astype(str).str.zfill(4)
    )

    # Optional: reorder columns to be more human-friendly
    column_order = [
        "OpenKIWAS_ID",
        "Search Phrase",
        "Title",
        "DOI",
        "Publication Date",
        "Publication Year",
        "Access",
        "Keywords",
        "License",
        "Version",
        "Description",
        "Creators",
        "Number of Files",
        "File Names",
        "File Sizes (bytes)",
        "File Links",
        "Views",
        "Downloads",
        "Subjects",
        "Communities",
        "Grants",
        "Search Query URL",
        "Dataset Page",
        "Metadata API URL",
        "Related Identifiers"
    ]

    # Keep only columns that actually exist (safety)
    column_order = [c for c in column_order if c in results_df.columns]
    results_df = results_df[column_order]

    csv_filename = "zenodo_results.csv"

    # Get current date and time
    timestamp = datetime.now().strftime("%Y-%m-%d_%H%M")

    # Split the filename into name and extension
    name, ext = csv_filename.rsplit('.', 1)
    
    # Create new filename with timestamp suffix
    csv_filename = f"{name}_{timestamp}.{ext}"

    results_df.to_csv(csv_filename, index=False, encoding="utf-8")

    print(f"\n✅ Search complete! Cleaned results saved to '{csv_filename}'")


# ----------------------------------------------------
# Run the script
# ----------------------------------------------------
if __name__ == "__main__":
    search_and_save_results(search_phrases, sort_by="mostrecent", years_filter=5)
