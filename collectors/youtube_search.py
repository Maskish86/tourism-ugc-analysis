from pathlib import Path
import os
import json
from googleapiclient.discovery import build
from dotenv import load_dotenv
from datetime import datetime, timedelta


OUTPUT_DIR = Path("data/raw/search")
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)


def youtube_search(query: str, max_requests=10, after=None):
    load_dotenv()
    API_KEY = os.getenv("YOUTUBE_API_KEY")
    if not API_KEY:
        raise ValueError("Please set YOUTUBE_API_KEY in .env")
    yt = get_youtube_client(API_KEY)

    print(f"Fetching search results for: {query}")
    results = run_split_search(yt, query, max_requests=max_requests, after=after)
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


def run_split_search(youtube, query: str, max_requests: int, after=None):
    collected = {}
    request_count = 0

    if after:
        start_year = datetime.fromisoformat(after).year
    else:
        start_year = datetime.now().year - 10

    end_year = datetime.now().year

    for year in range(end_year, start_year - 1, -1):
        if request_count >= max_requests:
            break

        published_after = datetime(year, 1, 1).isoformat("T") + "Z"
        published_before = datetime(year + 1, 1, 1).isoformat("T") + "Z"

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

        print(f"[{year}] Total requests: {request_count}, Collected: {len(collected)}")

        if request_count >= max_requests:
            break

    return list(collected.values())


def save_search_results(query: str, results):
    filename = f"{query}_search.json".replace(" ", "_")
    filepath = OUTPUT_DIR / filename

    with open(filepath, "w", encoding="utf-8") as f:
        json.dump(results, f, ensure_ascii=False, indent=2)

    print(f"Saved {len(results)} unique results for '{query}' → {filepath}")


if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument("--query", type=str, required=True)
    parser.add_argument("--max_requests", type=int, default=90)
    parser.add_argument("--after", type=str, default=None)
    args = parser.parse_args()

    youtube_search(
        query=args.query,
        max_requests=args.max_requests,
        after=args.after,
    )
