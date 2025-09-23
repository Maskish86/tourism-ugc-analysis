from pathlib import Path
import os
import json
import yaml
import pandas as pd
import re
from googleapiclient.discovery import build
from dotenv import load_dotenv
import isodate

with open("config/kawagoe_keywords.yaml", "r", encoding="utf-8") as f:
    keywords = yaml.safe_load(f)
NEGATIVE_KEYWORDS = keywords["negative_keywords"]

with open("config/youtube_category_map.yaml", "r", encoding="utf-8") as f:
    cfg = yaml.safe_load(f)
CATEGORY_MAP = cfg["category_map"]

with open("config/tourism_keyword_rules.yaml", "r", encoding="utf-8") as f: 
    KEYWORD_DICT = yaml.safe_load(f)

SEARCH_DIR = Path("data/raw/search")
PROCESSED_DIR = Path("data/processed")
PROCESSED_DIR.mkdir(parents=True, exist_ok=True)

def youtube_enricher(max_requests=100, batch_idx=0, min_views=5000):
    load_dotenv()
    API_KEY = os.getenv("YOUTUBE_API_KEY")
    if not API_KEY:
        raise ValueError("Please set YOUTUBE_API_KEY in .env")
    yt = build("youtube", "v3", developerKey=API_KEY)


    all_items = []
    search_files = SEARCH_DIR.glob("*_search.json")
    for file in search_files:
        print(f"Loading {file}")
        with open(file, "r", encoding="utf-8") as f:
            items = json.load(f)
        all_items.extend(items)

    df_all = pd.json_normalize(all_items).drop_duplicates(subset=["id.videoId"])
    df_all["text"] = df_all["snippet.title"].fillna("") + " " + df_all["snippet.description"].fillna("")
    pattern = re.compile("|".join(map(re.escape, NEGATIVE_KEYWORDS)), flags=re.IGNORECASE)
    mask_negative = ~df_all["text"].str.contains(pattern, na=False)
    df_tourism = df_all[mask_negative].reset_index(drop=True)
    print(f"Remaining {len(df_tourism)} videos after keyword filtering.")
   
    start = batch_idx * max_requests
    end = min((batch_idx + 1) * max_requests, len(df_tourism))
    df_tourism = df_tourism.iloc[start : end]
    print(f"Processing {len(df_tourism)} videos (rows {start}–{end})")

    final_df = enrich_videos_from_df(df_tourism, yt, min_views=min_views)
    filepath = PROCESSED_DIR / "youtube_video_details.parquet"
    if filepath.exists():
        existing_df = pd.read_parquet(filepath)
        final_df = pd.concat([existing_df, final_df]).drop_duplicates(subset=["video_id"])
    final_df.to_parquet(filepath, engine="pyarrow", index=False)
    print(f"Saved {len(final_df)} enriched records → {filepath}")


def get_video_details(youtube, video_ids, chunk_size=50):
    all_items = []
    for i in range(0, len(video_ids), chunk_size):
        batch_ids = video_ids[i:i+chunk_size]
        request = youtube.videos().list(
            part="snippet,contentDetails,statistics",
            id=",".join(batch_ids)
        )
        response = request.execute()
        all_items.extend(response.get("items", []))
    return {"items": all_items}


def enrich_videos_from_df(df_tourism, youtube, min_views, fetch_caps=True):
    video_ids = df_tourism["id.videoId"].tolist()
    details = get_video_details(youtube, video_ids)

    rows = []
    for item in details.get("items", []):
        stats = item.get("statistics", {})
        snippet = item["snippet"]
        content = item.get("contentDetails", {})

        view_count = int(stats.get("viewCount", 0))
        if view_count < min_views:
            continue

        row = {
            "video_id": item["id"],
            "title": snippet.get("title"),
            "description": snippet.get("description"),
            "publish_date": snippet.get("publishedAt"),
            "channel_id": snippet.get("channelId"),
            "channel_title": snippet.get("channelTitle"),
            "tags": snippet.get("tags", []),

            "view_count": view_count,
            "like_count": int(stats.get("likeCount", 0)),
            "comment_count": int(stats.get("commentCount", 0)),
            "favorite_count": int(stats.get("favoriteCount", 0)),
            "duration": isodate.parse_duration(content.get("duration")).total_seconds() if content.get("duration") else None,
            "definition": content.get("definition"),
            "category_id": snippet.get("categoryId"),
            "default_language": snippet.get("defaultLanguage"),
            "default_audio_language": snippet.get("defaultAudioLanguage"),
        }
        rows.append(row)

    df = pd.DataFrame(rows)
    df["category_id"] = df["category_id"].map(CATEGORY_MAP).fillna("Other")
    compiled_dict = {cat: re.compile("|".join(words), re.IGNORECASE) for cat, words in KEYWORD_DICT.items()}
    df["text"] = df["title"].fillna("") + " " + df["description"].fillna("")
    for cat, words in KEYWORD_DICT.items():
        pattern = "|".join(words)
        df[cat] = df["text"].str.contains(pattern, case=False, regex=True, na=False).astype(int)
    return df.drop(columns=["text"])



if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser(description="Fetch video details for saved search results.")
    parser.add_argument("--max_requests", type=int, default=100)
    parser.add_argument("--batch_idx", type=int, default=0)
    parser.add_argument("--min_views", type=int, default=5000)
    args = parser.parse_args()
    youtube_enricher(
        max_requests=args.max_requests,
        batch_idx=args.batch_idx,
        min_views=args.min_views,
    )

    