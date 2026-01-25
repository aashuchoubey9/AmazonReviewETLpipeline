from pyspark.sql.functions import col, trim, size
from pyspark.sql.types import StringType, ArrayType
from spark_session import get_spark_session

# ---------------------------
# Spark Session
# ---------------------------
spark = get_spark_session("Amazon-Ingest-Raw")

# ---------------------------
# HDFS Paths (Raw Layer)
# ---------------------------
BASE_PATH = "amazon-review-analytics/big-data-pipeline/data/raw"

REVIEWS_RAW_PATH = f"{BASE_PATH}/reviews/Appliances.json"
METADATA_RAW_PATH = f"{BASE_PATH}/metadata/meta_Appliances.json"

# ---------------------------
# Read Raw JSON from HDFS
# ---------------------------
reviews_df = spark.read.json(REVIEWS_RAW_PATH)
metadata_df = spark.read.json(METADATA_RAW_PATH)

# ---------------------------
# Select Required Columns
# ---------------------------
reviews_selected_df = reviews_df.select(
    col("reviewerID"),
    col("verified"),
    col("asin"),
    col("overall"),
    col("reviewText"),
    col("summary"),
    col("unixReviewTime")
)

metadata_selected_df = metadata_df.select(
    col("asin"),
    col("title"),
    col("brand"),
    col("category")
)

# ---------------------------
# Data Quality Checks (Detection Only)
# ---------------------------
for c in reviews_selected_df.columns:
    print(f"Reviews | {c} | NULL = {reviews_selected_df.filter(col(c).isNull()).count()}")

for c in metadata_selected_df.columns:
    print(f"Metadata | {c} | NULL = {metadata_selected_df.filter(col(c).isNull()).count()}")

print("Raw ingestion completed successfully")

