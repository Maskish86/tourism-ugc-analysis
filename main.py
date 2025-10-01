from collectors.youtube_search import youtube_search
from preprocess.youtube_enricher import youtube_enricher
from preprocess.youtube_captions import youtube_captions
from analysis.youtube_strategy import generate_video_report
from collectors.places_search import collect_nearby_places
from preprocess.places_cleaner import clean_places_data
from analysis.places_strategy import generate_tourism_report
from analysis.bq_table_builder import run_bq_sql

def analyze_youtube():
    youtube_search(query="川越", start_year=2020)
    youtube_search(query="Kawagoe", start_year=2020)
    youtube_enricher()
    youtube_captions()
    generate_video_report()

def analyze_gmap():
    collect_nearby_places()
    clean_places_data()
    generate_tourism_report()

if __name__ == "__main__":
    analyze_youtube()
    analyze_gmap()
    run_bq_sql()