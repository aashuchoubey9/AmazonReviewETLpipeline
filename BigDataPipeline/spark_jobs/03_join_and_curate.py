from pyspark.sql.functions import col, trim, size
from spark_session import get_spark_session

# -------------------------------------------------
# Spark Session
# -------------------------------------------------
spark = get_spark_session("Amazon-Join-And-Curate")

# -------------------------------------------------
# HDFS Paths (Silver Layer)
# -------------------------------------------------
BASE_PATH = "amazon-review-analytics/big-data-pipeline/data"

REVIEWS_CLEANED_PATH = f"{BASE_PATH}/cleaned/reviews"
METADATA_CLEANED_PATH = f"{BASE_PATH}/cleaned/metadata"

# -------------------------------------------------
# Read Cleaned Data
# -------------------------------------------------
cleaned_reviews_df = spark.read.parquet(REVIEWS_CLEANED_PATH)
cleaned_metadata_df = spark.read.parquet(METADATA_CLEANED_PATH)

# -------------------------------------------------
# Schema Validation (Defensive Programming)
# -------------------------------------------------
cleaned_reviews_df.printSchema()
cleaned_metadata_df.printSchema()

# -------------------------------------------------
# INNER JOIN on asin
# -------------------------------------------------
joined_df = cleaned_reviews_df.join(
    cleaned_metadata_df,
    on="asin",
    how="inner"
)

print("Joined count:", joined_df.count())

# -------------------------------------------------
# Business Rules (Gold Layer Enforcement)
# -------------------------------------------------
curated_df = joined_df.filter(
    # asin must be present
    col("asin").isNotNull() &
    (trim(col("asin")) != "") &

    # review text must be present
    col("reviewText").isNotNull() &
    (trim(col("reviewText")) != "") &

    # summary must be present (business decision)
    col("summary").isNotNull() &
    (trim(col("summary")) != "") &

    # rating must be present
    col("overall").isNotNull() &

    # brand must be present
    col("brand").isNotNull() &
    (trim(col("brand")) != "") &

    # category must exist and not be empty
    col("category").isNotNull() &
    (size(col("category")) > 0) &

    # only verified purchases
    (col("verified") == True)
)

print("Curated count after business rules:", curated_df.count())

# -------------------------------------------------
# Final Sanity Check
# -------------------------------------------------
curated_df.show(5, truncate=False)

print("Join & curation completed successfully")

