from pathlib import Path
import json
import pandas as pd
import os

JSON_DIR = Path("data/raw")
PROCESSED_DIR = Path("data/processed")
PROCESSED_DIR.mkdir(exist_ok=True)

def clean_places_data(rating_threshold=3.9):
    json_path = Path(JSON_DIR / "place_details.json")
    with open(json_path, "r", encoding="utf-8") as f:
        details = json.load(f)

    details = [p for p in details if p.get("rating", 0) >= rating_threshold]

    df_places = flatten_places(details)
    df_reviews = flatten_reviews(details)

    df_places.to_parquet(PROCESSED_DIR / "gmap_places.parquet", engine="pyarrow", index=False)
    df_reviews.to_parquet(PROCESSED_DIR / "gmap_reviews.parquet", engine="pyarrow", index=False)

    print(f"Saved {len(df_places)} places → gmap_places.parquet")
    print(f"Saved {len(df_reviews)} reviews → gmap_reviews.parquet")

def flatten_places(details):
    for p in details:
        p.pop("regularOpeningHours", None)
    df_places = pd.json_normalize(details).drop(columns=["reviews"], errors="ignore")
    df_places = df_places.rename(columns={
        "id": "place_id",
        "displayName.text": "name",
        "displayName.languageCode": "display_name_lang",
        "formattedAddress": "address",
        "location.latitude": "lat",
        "location.longitude": "lng",
        "userRatingCount": "rating_count",
        "editorialSummary.text": "summary",
        "editorialSummary.languageCode": "editorial_lang"
    })

    selected_labels = ["tourist_attraction", "food"]
    for label in selected_labels:
        df_places[label] = df_places["types"].apply(lambda x: int(label in x))
    return df_places

def flatten_reviews(details):
    all_reviews = []
    for p in details:
        pid = p.get("id")
        pname = p.get("displayName", {}).get("text")

        for r in p.get("reviews", []):
            all_reviews.append({
                "place_id": pid,
                "place_name": pname,
                "review_author": r.get("authorAttribution", {}).get("displayName"),
                "review_rating": r.get("rating"),
                "review_text": (
                    r.get("originalText", {}).get("text")
                    or r.get("text", {}).get("text") 
                ),
                "review_time": r.get("publishTime"),
                "review_language": (
                    r.get("originalText", {}).get("languageCode")
                    or r.get("text", {}).get("languageCode")
                )
            })
    return pd.DataFrame(all_reviews)

if __name__ == "__main__":
    clean_places_data()
