"""
Glue job: taxi-feed-framework   (config-driven, one script for every feed; 2 x G.1X)

The code never says "taxi". It opens s3://<BUCKET>/config/feeds/<FEED>.json and follows it:
read -> required columns not null -> dedupe -> add _loaded_at -> write -> audit row.
New feed = one new JSON file, no code change.

Job parameters
  --BUCKET   faiz-taxi-lake-2026   (NO s3:// here, because boto3 wants only the bucket name)
  --FEED     taxi_zones / yellow_trips / green_trips ...
"""
import sys, json, boto3
from datetime import datetime
from awsglue.utils import getResolvedOptions
from awsglue.context import GlueContext
from pyspark.context import SparkContext
from pyspark.sql import functions as F

# 1. Which feed to run? (comes from job parameters)
args = getResolvedOptions(sys.argv, ["BUCKET", "FEED"])
BUCKET, FEED = args["BUCKET"], args["FEED"]
spark = GlueContext(SparkContext.getOrCreate()).spark_session
spark.conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")
start = datetime.now()

# 2. Open the feed's form (JSON) from S3
obj = boto3.client("s3").get_object(Bucket=BUCKET, Key=f"config/feeds/{FEED}.json")
cfg = json.loads(obj["Body"].read())
print("Config:", cfg)

# 3. READ: the format comes from the form
reader = spark.read.format(cfg["format"])
if cfg["format"] == "csv":
    reader = reader.option("header", "true").option("inferSchema", "true")
df = reader.load(f"s3://{BUCKET}/{cfg['source_path']}")
raw_count = df.count()

# 4. QUALITY: required columns must not be empty
rule = " AND ".join(f"`{c}` IS NOT NULL" for c in cfg["required_columns"])
good, bad = df.filter(rule), df.filter(f"NOT ({rule})")
if cfg["dedupe"]:
    good = good.dropDuplicates()

# 5. WRITE: the location and partitions come from the form
writer = good.withColumn("_loaded_at", F.current_timestamp()).write.mode("overwrite")
if cfg["partition_by"]:
    writer = writer.partitionBy(*cfg["partition_by"])
writer.parquet(f"s3://{BUCKET}/{cfg['target_path']}")

# 6. AUDIT: record what happened (one row per run)
good_count, bad_count = good.count(), bad.count()
spark.createDataFrame([(FEED, raw_count, good_count, bad_count, str(start), str(datetime.now()))],
    "feed string, raw_rows long, good_rows long, bad_rows long, started string, ended string") \
    .write.mode("append").parquet(f"s3://{BUCKET}/framework/audit/")
print(f"{FEED}: raw={raw_count} good={good_count} bad={bad_count}")
