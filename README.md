## Scala Spark Transformation for Top Detected Items by Location
This script identifies the most frequently detected items within each geographical location.\
<br/><br/>
**Note:** A minor schema change has been made for the output Parquet file (Parquet File 3).

From:\
`geographical_location_oid`: bigint (A unique bigint identifier for the geographical location)\
To:\
`geographical_location_oid`: varchar(500) (Geographical location name)
<br/><br/>
* **Dataset A (Parquet File 1):**
    * Schema:
        * `geographical_location_oid`: bigint (Unique identifier for location)
        * `video_camera_oid`: bigint (Unique identifier for video camera)
        * `detection_oid`: bigint (Unique identifier for detection event) (Duplicates exist due to ingestion error, count each only once)
        * `item_name`: varchar(5000) (Item name)
        * `timestamp_detected`: bigint (Timestamp of detection)
        * Size: ~1 million rows
* **Dataset B (Parquet File 2):**
    * Schema:
        * `geographical_location_oid`: bigint (Unique identifier for location)
        * `geographical_location`: varchar(500) (Geographical location name)
        * Size: 10000 rows

**Output:**

* **Parquet File 3:**
    * Schema:
        * `geographical_location`: varchar(500) (Geographical location name)
        * `item_rank`: varchar(500) (Rank - 1 being most popular)
        * `item_name`: varchar(5000) (Item name)
