DROP TABLE IF EXISTS `${PROJECT_ID}.${BQ_DATASET}.youtube_video_details`;

CREATE OR REPLACE EXTERNAL TABLE `${PROJECT_ID}.${BQ_DATASET}.youtube_video_details`
OPTIONS (
  format = 'PARQUET',
  uris = ['gs://${GCS_BUCKET}/processed/*youtube_video_details.parquet']
);

DROP TABLE IF EXISTS `${PROJECT_ID}.${BQ_DATASET}.youtube_video_features`;

CREATE OR REPLACE TABLE `${PROJECT_ID}.${BQ_DATASET}.youtube_video_features` AS
WITH base AS (
  SELECT
    video_id,
    title,
    description,
    view_count,
    like_count,
    comment_count,
    ARRAY_TO_STRING(
      ARRAY(
        SELECT t.element
        FROM UNNEST(tags.list) AS t
      ), ', '
    ) AS tags,
    category_id,
    default_language,
    default_audio_language,
    duration,
    heritage,
    food, 
    events,
    nature,

    SAFE_DIVIDE(CAST(like_count AS INT64), NULLIF(CAST(view_count AS INT64), 0)) AS like_ratio,
    SAFE_DIVIDE(CAST(comment_count AS INT64), NULLIF(CAST(view_count AS INT64), 0)) AS comment_ratio,
    SAFE_DIVIDE(CAST(view_count AS INT64),
                GREATEST(DATE_DIFF(CURRENT_DATE(), DATE(TIMESTAMP_MICROS(CAST(publish_date / 1000 AS INT64))), DAY), 1)) AS views_per_day_avg,

    CASE
      WHEN view_count < 10000 THEN "1k-10k"
      WHEN view_count < 100000 THEN "10k-100k"
      ELSE "100k+"
    END AS view_count_bin,

    -- Content category
    CASE
      WHEN nature = 1 THEN  "nature"
      WHEN events = 1 THEN "events"
      WHEN heritage = 1 THEN "heritage"
      WHEN food = 1 THEN "food"
      ELSE "other"
    END AS content_category,


    DATE(TIMESTAMP_MICROS(CAST(publish_date / 1000 AS INT64))) AS publish_date,
    EXTRACT(MONTH FROM DATE(TIMESTAMP_MICROS(CAST(publish_date / 1000 AS INT64)))) AS publish_month,  
    EXTRACT(DAYOFWEEK FROM DATE(TIMESTAMP_MICROS(CAST(publish_date / 1000 AS INT64)))) AS publish_day_of_week,

    CASE 
      WHEN default_language IN ('ja') 
      THEN 1 ELSE 0 
    END AS is_japanese,

    CASE
      WHEN CAST(duration AS INT64) <= 90 
      THEN 1 ELSE 0
    END AS is_short_video,

  FROM `${PROJECT_ID}.${BQ_DATASET}.youtube_video_details`
)

SELECT *
FROM base;
