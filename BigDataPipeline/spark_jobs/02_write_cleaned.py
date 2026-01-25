from pyspark.sql.functions import col
from spark_session import get_spark_session

# -------------------------------------------------
# Spark Session
# -------------------------------------------------
spark = get_spark_session("Amazon-Write-Cleaned")

# -------------------------------------------------
# HDFS Paths
# -------------------------------------------------
BASE_PATH = "amazon-review-analytics/big-data-pipeline/data"

REVIEWS_RAW_PATH = f"{BASE_PATH}/raw/reviews/Appliances.json"
METADATA_RAW_PATH = f"{BASE_PATH}/raw/metadata/meta_Appliances.json"

REVIEWS_CLEANED_PATH = f"{BASE_PATH}/cleaned/reviews"
METADATA_CLEANED_PATH = f"{BASE_PATH}/cleaned/metadata"

# -------------------------------------------------
# Read Raw Data
# -------------------------------------------------
reviews_df = spark.read.json(REVIEWS_RAW_PATH)
metadata_df = spark.read.json(METADATA_RAW_PATH)

# -------------------------------------------------
# Select Required Columns (Schema Control)
# -------------------------------------------------
reviews_cleaned_df = reviews_df.select(
    col("reviewerID"),
    col("verified"),
    col("asin"),
    col("overall"),
    col("reviewText"),
    col("summary"),
    col("unixReviewTime")
)

metadata_cleaned_df = metadata_df.select(
    col("asin"),
    col("title"),
    col("brand"),
    col("category")
)

# -------------------------------------------------
# Explicitly drop 'price' if it exists (Safety)
# -------------------------------------------------
if "price" in reviews_cleaned_df.columns:
    reviews_cleaned_df = reviews_cleaned_df.drop("price")

if "price" in metadata_cleaned_df.columns:
    metadata_cleaned_df = metadata_cleaned_df.drop("price")

# -------------------------------------------------
# Write Cleaned Data (Silver Layer)
# -------------------------------------------------
reviews_cleaned_df.write \
    .mode("overwrite") \
    .parquet(REVIEWS_CLEANED_PATH)

metadata_cleaned_df.write \
    .mode("overwrite") \
    .parquet(METADATA_CLEANED_PATH)

print("Cleaned data written successfully")

