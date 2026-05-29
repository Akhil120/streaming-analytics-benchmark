-- Phase 4: StarRocks external Paimon catalog
-- Points at the same cold-lake Parquet files that Flink P3 reads.
-- Run once via: make sr-init

CREATE EXTERNAL CATALOG IF NOT EXISTS paimon_catalog
COMMENT "Fluss cold lake — Paimon Parquet on MinIO"
PROPERTIES (
  "type"                              = "paimon",
  "paimon.catalog.type"               = "filesystem",
  "paimon.catalog.warehouse"          = "s3://fluss/paimon-warehouse",
  "aws.s3.use_aws_sdk_default_behavior" = "false",
  "aws.s3.enable_path_style_access"   = "true",
  "aws.s3.access_key"                 = "minioadmin",
  "aws.s3.secret_key"                 = "minioadmin",
  "aws.s3.endpoint"                   = "http://minio:9000",
  "aws.s3.region"                     = "us-east-1"
);
