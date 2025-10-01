import pandas as pd
import re
import os
from google import genai
import textwrap
from pathlib import Path
from dotenv import load_dotenv

load_dotenv()
PROCESSED_DIR = Path("data/processed")
OUTPUT_DIR = Path("outputs")
OUTPUT_DIR.mkdir(exist_ok=True)

GCP_PROJECT_ID = os.getenv("GCP_PROJECT_ID")
client = genai.Client(
    vertexai=True,
    project=GCP_PROJECT_ID,
    location="us-central1"
)

def generate_tourism_report(max_places=20):
    df_reviews = pd.read_parquet(PROCESSED_DIR / "gmap_reviews.parquet")
    df_reviews["cleaned_review"] = df_reviews["review_text"].apply(lambda x: re.sub(r"\s+", " ", x))
    df_reviews["review_month"] = pd.to_datetime(df_reviews["review_time"]).dt.month
    df_reviews = df_reviews.drop(["review_author","review_language"], axis=1)
    df_places = pd.read_parquet(PROCESSED_DIR / "gmap_places.parquet")
    df_places = df_places[["place_id", "name", "rating", "rating_count", "summary"]]
    df_places = df_places.sort_values("rating_count", ascending=False)
    prompts = []
    for place_id in df_places["place_id"][:max_places]:
        prompt = build_place_prompt(place_id, df_places, df_reviews)
        prompts.append(prompt)
    print(f"Generating report for {len(prompts)} places...")
    generated_text = generate_tourism_strategy("\n\n".join(prompts), max_places)
    out_text_file = OUTPUT_DIR / "generated_tourism_report.txt"
    with open(out_text_file, "w", encoding="utf-8") as f:
        f.write(generated_text)
    print(f"Saved generated report to {out_text_file}")

    wrapped_text = wrap_preserve_newlines(generated_text, width=100)
    out_md_file = OUTPUT_DIR / "generated_tourism_report.md"
    with open(out_md_file, "w") as f:
        f.write(wrapped_text)
    print(f"Saved generated report to {out_md_file}")

def build_place_prompt(place_id, df_places, df_reviews):
    place = df_places[df_places["place_id"] == place_id].iloc[0]
    place_header = (
        f"{place['name']} | "
        f"{place['rating']}⭐ ({place['rating_count']}) | "
        f"{place['summary'] or ''}"
    )

    reviews = (
        df_reviews[df_reviews["place_id"] == place_id]
        .sort_values("review_time", ascending=False).head(5)
    )

    reviews_text = "\n".join(
        [f"[M{row.review_month}] {row.cleaned_review}" for row in reviews.itertuples()]
    )

    block = f"Place: {place_header}\nReviews:\n{reviews_text}"
    return block 

def generate_tourism_strategy(prompts_text, max_places):
    prompt_header = f"""
    あなたは観光戦略の専門家です。
    以下のグーグルマップのレビューを分析し、その地域の観光資源を最大限に活用できる
    プロモーション戦略を提案してください。
    出力はMarkdownを絶対に使用せず、通常の日本語の文章スタイルで書いてください。
    必ず{2000 + max_places * 100 }文字以上で中途半端に打ち切らず、最後まで書き切ってください。

    出力には必ず以下の観点を含めてください：

    1. 観光コンテンツのトレンド分析
    - レビューから読み取れる観光体験や人気要素
    - 季節ごとの魅力やイベント傾向
    - 訪問者が特に感動したり不満を持ったりした特徴的なポイント

    2. ターゲット分析
    - 国内観光客とインバウンド観光客の関心の違い
    - 年齢層・旅行スタイル（カップル、ファミリー、シニア、学生）による嗜好の傾向
    - 訪問者が重視している価値観（写真映え、非日常感、学び、リラックスなど）

    3. プロモーション戦略
    - 季節別・ターゲット層別に効果的な施策
    - レビューで好評だった体験や魅力を活かしたキャンペーン企画
    - 不満点や課題を解消する改善提案（混雑対策、アクセス、設備など）
    """
    
    max_output_tokens = min(32000, 3000 + 800 * max_places)
    response = client.models.generate_content(
        model="gemini-2.5-pro",   
        contents=prompt_header + prompts_text,
        config={
            "max_output_tokens": max_output_tokens,
            "temperature": 0.4,
            "top_p": 0.8,
            "top_k": 40,
        }
    )
    print(response)
    return response.text.strip()


def wrap_preserve_newlines(text: str, width: int = 100) -> str:
    return "\n".join(
        textwrap.fill(line, width=width) if line.strip() else ""
        for line in text.splitlines()
    )


if __name__ == "__main__":
    generate_tourism_report()
