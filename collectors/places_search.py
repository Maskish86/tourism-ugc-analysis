import os
import json
from pathlib import Path
import time
import googlemaps
import requests
from dotenv import load_dotenv


load_dotenv()

API_KEY = os.getenv("GOOGLE_MAPS_API_KEY")
if not API_KEY:
    raise RuntimeError("Missing GOOGLE_MAPS_API_KEY environment variable")

KAWAGOE_LOCATION = (35.9251, 139.4856)
TYPES = [None, "tourist_attraction", "restaurant"]

RAW_DIR = Path("data/raw")
RAW_DIR.mkdir(exist_ok=True)
gmaps = googlemaps.Client(key=API_KEY)

def collect_nearby_places(search_radius=4000, max_pages=3, max_results=60):
    results = fetch_nearby_places(search_radius, max_pages)
    top_places = pick_top_places(results, max_results)
    details = fetch_place_details([p["place_id"] for p in top_places])
    save_path = RAW_DIR / "place_details.json"
    with open(save_path, "w", encoding="utf-8") as f:
        json.dump(details, f, ensure_ascii=False, indent=2)
    print(f"Saved place details to {save_path}")
    return results


def fetch_nearby_places(search_radius=3000, max_pages=3):
    all_results = []

    for place_type in TYPES:
        results = []
        if place_type:
            page = gmaps.places_nearby(location=KAWAGOE_LOCATION, radius=search_radius, type=place_type)
        else:
            page = gmaps.places_nearby(location=KAWAGOE_LOCATION, radius=search_radius)

        results.extend(page["results"])

        pages_fetched = 1
        while "next_page_token" in page and pages_fetched < max_pages:
            time.sleep(2)  
            page = gmaps.places_nearby(page_token=page["next_page_token"])
            results.extend(page["results"])
            pages_fetched += 1

        print(f"{place_type or 'no type'} → {len(results)} results ({pages_fetched} pages)")
        all_results.extend(results)

    seen, deduped = set(), []
    for p in all_results:
        pid = p.get("place_id")
        if pid and pid not in seen:
            seen.add(pid)
            deduped.append(p)

    print(f"Total {len(deduped)} unique places from {len(TYPES)} searches")
    return deduped

def pick_top_places(results, limit=60, ratio_popularity=0.8, min_reviews=200):
    filtered = [p for p in results if p.get("user_ratings_total", 0) >= min_reviews]
    n_popularity = int(limit * ratio_popularity)
    n_quality = limit - n_popularity

    top_popularity = sorted(
        filtered, key=lambda p: p.get("user_ratings_total", 0), reverse=True
    )[:n_popularity]

    sorted_quality = sorted(
        filtered,
        key=lambda p: (p.get("rating", 0), p.get("user_ratings_total", 0)),
        reverse=True
    )

    seen = {p.get("place_id") for p in top_popularity}
    top_quality = []
    for p in sorted_quality:
        pid = p.get("place_id")
        if pid not in seen:
            top_quality.append(p)
            seen.add(pid)
        if len(top_quality) >= n_quality:
            break

    return top_popularity + top_quality


def fetch_place_details(place_ids):
    details = []
    for pid in place_ids:
        url = f"https://places.googleapis.com/v1/places/{pid}"
        headers = {
            "X-Goog-Api-Key": API_KEY,
            "X-Goog-FieldMask": (
                "id,displayName,formattedAddress,location,"
                "regularOpeningHours,rating,userRatingCount,"
                "reviews,types,priceLevel,editorialSummary"
            ),
            "Accept-Language": "ja" 
        }
        resp = requests.get(url, headers=headers)
        resp.raise_for_status()
        details.append(resp.json())
    print(f"Retrieved details for {len(details)} places")
    return details


if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument("--search_radius", type=int, default=4000)
    parser.add_argument("--max_pages", type=int, default=3)
    parser.add_argument("--max_results", type=int, default=60)
    args = parser.parse_args()

    collect_nearby_places(
        search_radius=args.search_radius,
        max_pages=args.max_pages,
        max_results=args.max_results
    )