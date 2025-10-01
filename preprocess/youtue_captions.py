from pathlib import Path
import pandas as pd
from youtube_transcript_api import YouTubeTranscriptApi, TranscriptsDisabled, NoTranscriptFound
import time, random

PROCESSED_DIR = Path("data/processed")

def youtube_captions(max_fetches=10):
    df = pd.read_parquet(PROCESSED_DIR /"youtube_video_details.parquet")
    df["view_count"] = pd.to_numeric(df["view_count"], errors="coerce")
    df = df.sort_values("view_count", ascending=False).head(max_fetches)
    print(f"Fetching captions for {len(df)} videos with top views")
    captions = []
    for _, row in df.iterrows():
        video_id = row["video_id"]
        caps = fetch_captions(video_id, languages=['ja', 'en'])
        captions.append(caps)
    df["caption"] = captions
    df = df[df["caption"].notnull()]
    print(f"Saved {len(df)} videos with captions")
    df.to_parquet(PROCESSED_DIR / "youtube_captions.parquet")

def fetch_captions(video_id, languages=['ja', 'en']):
    time.sleep(random.uniform(5, 15))  
    api = YouTubeTranscriptApi() 
    try:
        transcript = api.fetch(video_id, languages=languages)
        return " ".join(seg.text for seg in transcript)  
    except NoTranscriptFound:
        try:
            transcript = api.fetch(video_id, languages=[f"a.{lang}" for lang in languages])
            return " ".join(seg.text for seg in transcript)  
        except NoTranscriptFound:
            print(f"No transcript found for {video_id} in {languages} or auto")
            return None
    except TranscriptsDisabled:
        print(f"Transcripts disabled for {video_id}")
        return None
    except Exception as e:
        print(f"Error fetching captions for {video_id}: {e}")
        return None

if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument("--max_fetches", type=int, default=100)
    args = parser.parse_args()
    youtube_captions(max_fetches=args.max_fetches)

