DECLARE center_lat FLOAT64 DEFAULT 35.9251;   -- 時の鐘の緯度
DECLARE center_lng FLOAT64 DEFAULT 139.4852;  -- 時の鐘の経度

DROP TABLE IF EXISTS `${PROJECT_ID}.${BQ_DATASET}.gmap_place_details`;

CREATE OR REPLACE EXTERNAL TABLE `${PROJECT_ID}.${BQ_DATASET}.gmap_place_details`
OPTIONS (
  format = 'PARQUET',
  uris = ['gs://${GCS_BUCKET}/processed/gmap_places.parquet']
);

DROP TABLE IF EXISTS `${PROJECT_ID}.${BQ_DATASET}.gmap_place_features`;

CREATE OR REPLACE TABLE `${PROJECT_ID}.${BQ_DATASET}.gmap_place_features` AS
WITH base AS (
  SELECT
    place_id,
    name,
    address,
    rating,
    rating_count,
    rating * LOG(1 + rating_count) AS weighted_rating,
    lat,
    lng,
    ST_GEOGPOINT(lng, lat) AS location,
    display_name_lang,
    summary,
    editorial_lang,
    priceLevel,
    tourist_attraction,
    food,

    ARRAY_TO_STRING(
      ARRAY(
        SELECT t.element
        FROM UNNEST(types.list) AS t
      ), ', '
    ) AS types,

    ST_DISTANCE(
        ST_GEOGPOINT(lng, lat),
        ST_GEOGPOINT(center_lng, center_lat)
    ) / 1000 AS dist_km,

    CASE
        WHEN ST_DISTANCE(ST_GEOGPOINT(lng, lat), ST_GEOGPOINT(center_lng, center_lat)) <= 1000 THEN "Central Kawagoe"
        WHEN ST_DISTANCE(ST_GEOGPOINT(lng, lat), ST_GEOGPOINT(center_lng, center_lat)) <= 3000 THEN "Inner Kawagoe"
        ELSE "Outer Kawagoe"
    END AS kawagoe_zone,

    CASE
        WHEN lat >= center_lat AND lng >= center_lng THEN "Northeast Kawagoe"
        WHEN lat >= center_lat AND lng <  center_lng THEN "Northwest Kawagoe"
        WHEN lat <  center_lat AND lng >= center_lng THEN "Southeast Kawagoe"
        ELSE "Southwest Kawagoe"
    END AS kawagoe_quadrant,

FROM `${PROJECT_ID}.${BQ_DATASET}.gmap_place_details`
)

SELECT * FROM base;