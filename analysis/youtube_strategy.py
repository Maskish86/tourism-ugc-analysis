import pandas as pd
import re
import os
from google import genai
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


def generate_video_report(max_videos=20, max_chars=100_000):
    df = pd.read_parquet(PROCESSED_DIR / "youtube_captions.parquet")
    df = df.sort_values("view_count", ascending=False)
    
    prompts = []
    current_length = 0
    for video_id in df["video_id"][:max_videos]:
        prompt = build_video_prompt(video_id, df)
        if current_length + len(prompt) + 2 > max_chars:
            break
        prompts.append(prompt)
        current_length += len(prompt) + 2
    prompts_text = "\n\n".join(prompts)

    print(f"Generating report for {len(prompts)} videos...")
    generated_text = generate_tourism_strategy(prompts_text, len(prompts))
    out_file = OUTPUT_DIR / "generated_video_report.txt"
    with open(out_file, "w", encoding="utf-8") as f:
        f.write(generated_text)
    print(f"Saved generated report to {out_file}")


def build_video_prompt(video_id, df):
    video = df[df["video_id"] == video_id].iloc[0]    
    caption = clean_caption(video['caption'])
    title = video['title'].split("#")[0].strip()
    caption_summary = generate_caption_summary(video['title'], caption)
    block = (
        f"『{title}』 "
        f"(Views:{video['view_count']}, Likes:{video['like_count']}, Date:{video['publish_date'].date() or ''})\n"
        f"{caption_summary}"
    )
    return block


def clean_caption(captions: str) -> str:
    captions = re.sub(r"\s+", " ", captions)
    captions = re.sub(r"\[.*?\]", "", captions)
    captions = re.sub(r"\(.*?\)", "", captions)
    captions = re.sub(r"[♪★☆※]+", "", captions)
    captions = re.sub(r"ー{2,}", "ー", captions)  
    captions = re.sub(r"…{2,}", "…", captions)  
    captions = re.sub(r"\s+", " ", captions)
    return captions.strip()


def generate_caption_summary(title, caption):
    prompt = f"""
    動画『{title}』の字幕から川越観光に関連する内容を日本語でまとめてください。  
    観光体験の手順や訪問者の行動・感想をまとめてください。  
    特に観光体験・季節イベント、観光客のタイプ、反応、食事・アクセスに注目してください。  
    注意: 情報が存在しない項目は省略し、不要な雑談や効果音は書かないでください。
    字幕：
    {caption}
    """
    max_output_tokens = 1200 + len(caption)//8
    response = client.models.generate_content(
        model="gemini-2.5-flash",
        contents=prompt,
        config={
            "max_output_tokens": max_output_tokens,
            "temperature": 0.4,
            "top_p": 0.8,
            "top_k": 40,
        }
    )
    return response.text.strip()


def generate_tourism_strategy(prompts_text, num_videos):
    prompt_header = f"""
    あなたは観光戦略の専門家です。
    以下のYoutubeのデータを分析し、その地域の観光資源を最大限に活用できる
    プロモーション戦略を提案してください。
    出力はMarkdownを絶対に使用せず、通常の日本語の文章スタイルで書いてください。
    必ず{num_videos * 250}文字以上で中途半端に打ち切らず、最後まで書き切ってください。

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
    max_output_tokens = min(32000, 3000 + 800 * num_videos)
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

if __name__ == "__main__":
    generate_video_report()