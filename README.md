# 観光UGC分析（tourism-ugc-analysis）

このリポジトリは川越観光に関するユーザー生成コンテンツ（UGC）の分析を行うPythonプロジェクトです。

## インストール方法

1. Python（推奨バージョン: 3.8以上）をインストールしてください。
2. 必要なライブラリをインストールします：

```bash
pip install -r requirements.txt
```

## 環境変数の設定

このプロジェクトでは .env ファイルを使用してGoogle Cloudの認証情報やプロジェクト設定を管理しています。
ルートディレクトリに .env ファイルを作成し、以下の内容を記載してください：

```bash
# GCP プロジェクトID
GCP_PROJECT_ID=your-gcp-project-id

# BigQuery データセット名
BQ_DATASET=ugc_analysis

# Google Cloud Storage バケット名（gs:// は不要）
GCS_BUCKET=your-gcs-bucket-name

# APIキー
YOUTUBE_API_KEY=your-youtube-api-key
GOOGLE_MAPS_API_KEY=your-google-maps-api-key
```

## 使い方

main.pyを実行してください

```bash
python main.py
```