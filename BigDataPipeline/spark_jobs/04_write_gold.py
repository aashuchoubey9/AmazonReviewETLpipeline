from pyspark.sql.functions import col, trim, size
from spark_session import get_spark_session

# -------------------------------------------------
# Spark Session
# -------------------------------------------------
spark = get_spark_session("Amazon-Write-Gold")

# -------------------------------------------------
# HDFS Paths
# -------------------------------------------------
BASE_PATH = "amazon-review-analytics/big-data-pipeline/data"

REVIEWS_CLEANED_PATH = f"{BASE_PATH}/cleaned/reviews"
METADATA_CLEANED_PATH = f"{BASE_PATH}/cleaned/metadata"
GOLD_OUTPUT_PATH = f"{BASE_PATH}/processed/appliance_reviews_curated"

# -------------------------------------------------
# Read Silver Data
# -------------------------------------------------
reviews_df = spark.read.parquet(REVIEWS_CLEANED_PATH)
metadata_df = spark.read.parquet(METADATA_CLEANED_PATH)

# -------------------------------------------------
# Join Data
# -------------------------------------------------
joined_df = reviews_df.join(
    metadata_df,
    on="asin",
    how="inner"
)

# -------------------------------------------------
# Apply Business Rules (Same as Step 3)
# -------------------------------------------------
gold_df = joined_df.filter(
    col("asin").isNotNull() &
    (trim(col("asin")) != "") &

    col("reviewText").isNotNull() &
    (trim(col("reviewText")) != "") &

    col("summary").isNotNull() &
    (trim(col("summary")) != "") &

    col("overall").isNotNull() &

    col("brand").isNotNull() &
    (trim(col("brand")) != "") &

    col("category").isNotNull() &
    (size(col("category")) > 0) &

    (col("verified") == True)
)

# -------------------------------------------------
# Write Gold Data
# -------------------------------------------------
gold_df.write \
    .mode("overwrite") \
    .parquet(GOLD_OUTPUT_PATH)

print("Gold data written successfully")

