from pathlib import Path
import os
import json
from googleapiclient.discovery import build
from dotenv import load_dotenv
from datetime import datetime


OUTPUT_DIR = Path("data/raw/search")
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)


def youtube_search(query: str, max_requests=10, start_year=None, end_year=None):
    load_dotenv()
    API_KEY = os.getenv("YOUTUBE_API_KEY")
    if not API_KEY:
        raise ValueError("Please set YOUTUBE_API_KEY in .env")
    yt = get_youtube_client(API_KEY)

    print(f"Fetching search results for: {query}")
    results = run_split_search(yt, query, max_requests, start_year=start_year, end_year=end_year)
    save_search_results(query, results)


def get_youtube_client(api_key: str):
    return build("youtube", "v3", developerKey=api_key)


def run_search(youtube, query: str, published_after=None, published_before=None, request_budget=10):
    all_items = []
    next_page_token = None
    requests_used = 0

    while requests_used < request_budget:
        request = youtube.search().list(
            q=query,
            part="id,snippet",
            type="video",
            order="date",
            maxResults=50,
            publishedAfter=published_after,
            publishedBefore=published_before,
            pageToken=next_page_token
        )
        response = request.execute()
        requests_used += 1

        items = response.get("items", [])
        for item in items:
            item["query"] = query
        all_items.extend(items)

        next_page_token = response.get("nextPageToken")
        if not next_page_token:
            break

    return all_items, requests_used


def run_split_search(youtube, query: str, max_requests: int, start_year=None, end_year=None):
    collected = {}
    request_count = 0

    if end_year:
        end_year = int(end_year)
    else:
        end_year = datetime.now().year

    if start_year:
        start_year = int(start_year)
    else:
        start_year = end_year - 1

    quarter_starts = [(1,1), (4,1), (7,1), (10,1)]
    quarter_ends   = [(4,1), (7,1), (10,1), (1,1)] 

    for year in range(end_year, start_year - 1, -1):
        for q in range(4):
            if request_count >= max_requests:
                break

            published_after = datetime(year, *quarter_starts[q]).isoformat("T") + "Z"

            if q < 3:
                published_before = datetime(year, *quarter_ends[q]).isoformat("T") + "Z"
            else:
                published_before = datetime(year + 1, *quarter_ends[q]).isoformat("T") + "Z"

            items, used_requests = run_search(
                youtube,
                query,
                published_after=published_after,
                published_before=published_before,
                request_budget=max_requests - request_count
            )

            request_count += used_requests

            for item in items:
                vid = item["id"]["videoId"]
                collected[vid] = item

            print(f"[{year} Q{q+1}] Total requests: {request_count}, Collected: {len(collected)}")

            if request_count >= max_requests:
                break

    return list(collected.values())


def save_search_results(query: str, results):
    filename = f"{query}_search.json".replace(" ", "_")
    filepath = OUTPUT_DIR / filename

    if filepath.exists():
        with open(filepath, "r", encoding="utf-8") as f:
            try:
                existing = json.load(f)
            except json.JSONDecodeError:
                existing = []
    else:
        existing = []

    combined = {item["id"]["videoId"]: item for item in existing}
    for item in results:
        vid = item["id"]["videoId"]
        combined[vid] = item

    sorted_items = sorted(
        combined.values(),
        key=lambda x: datetime.fromisoformat(
            x["snippet"]["publishedAt"].replace("Z", "+00:00")
        ),
        reverse=True
    )

    with open(filepath, "w", encoding="utf-8") as f:
        json.dump(sorted_items, f, ensure_ascii=False, indent=2)

    print(f"Saved {len(sorted_items)} unique results for '{query}' → {filepath}")


if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument("--query", type=str, required=True)
    parser.add_argument("--max_requests", type=int, default=90)
    parser.add_argument("--start_year", type=int, default=None)
    parser.add_argument("--end_year", type=int, default=None)
    args = parser.parse_args()

    youtube_search(
        query=args.query,
        max_requests=args.max_requests,
        start_year=args.start_year,
        end_year=args.end_year,
    )
