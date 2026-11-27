# AWS-Based Orders ETL Pipeline using AWS Glue

## Project Overview

This project implements an ETL (Extract, Transform, Load) pipeline using **AWS Glue, Amazon S3, and Delta Lake** to process raw order data.

The pipeline reads order records from Amazon S3, performs data cleaning and transformations, applies data quality checks, and generates two separate outputs:

- **Parquet:** Cleaned and filtered order-level data.
- **Delta Lake:** Item-wise aggregated sales data.

The pipeline is designed to support incremental processing using AWS Glue Job Bookmarks.

## Architecture

```text
             Raw CSV Files
                   |
                   v
              Amazon S3
             (source/)
                   |
                   v
             AWS Glue ETL
                   |
             Drop Duplicates
                   |
              Change Schema
                   |
             Custom Transform
          (Calculate Total_Value)
                   |
              SQL Filter
          (Total_Value > 1000)
                   |
             Data Quality
                   |
          Select From Collection
                   |
                   v
          Amazon S3 (Parquet)
           (destination/)


             Aggregate by Item
                   |
              Rename Field
                   |
                   v
          Amazon S3 (Delta Lake)
          (destinationagg/)
```

## AWS Services Used

| Service | Purpose |
|---|---|
| Amazon S3 | Stores raw CSV files and processed outputs |
| AWS Glue | Performs ETL operations |
| AWS Glue Studio | Builds the ETL pipeline using Visual ETL |
| AWS Glue Data Quality | Validates data quality |
| AWS Glue Job Bookmarks | Supports incremental file processing |
| Delta Lake | Stores aggregated data in Delta format |
| Amazon CloudWatch | Used for job execution logs and troubleshooting |

## ETL Transformations

### 1. Data Ingestion
- Reads raw order data from Amazon S3.
- Uses CSV as the source format.

### 2. Data Cleaning
- Removes duplicate records.
- Applies schema mapping and data type conversions.

### 3. Custom Transformation
Creates a new column called `Total_Value`:

```python
Total_Value = price * quantity
```

### 4. Data Filtering
Filters records where:

```sql
Total_Value > 1000
```

### 5. Data Quality
Applies data quality rules to validate the processed records.

### 6. Data Aggregation
Groups records by `item` and calculates the sum of `Total_Value`.

The aggregated column is renamed to `Total_value`.

## Output Structure

```text
ordersetlbucket/
│
├── source/
│   └── orders.csv
│
├── destination/
│   └── Parquet output
│
└── destinationagg/
    └── Delta Lake output
```

**Output 1 – Parquet**

Contains cleaned and filtered order-level records.

**Output 2 – Delta Lake**

Contains item-wise aggregated sales values.

## Incremental Processing

AWS Glue Job Bookmarks are enabled to track previously processed source files.

When new CSV files are uploaded to the S3 source location, Glue Bookmarks help avoid reprocessing files that have already been successfully processed.

## Error Monitoring

Amazon CloudWatch Logs are used to inspect Glue job execution, identify transformation errors, and troubleshoot failures.

## Key Learning Outcomes

- Building ETL pipelines using AWS Glue Studio Visual ETL.
- Working with Amazon S3 as a data source and destination.
- Applying data cleaning and transformation operations.
- Implementing data quality checks.
- Performing aggregations using AWS Glue.
- Working with Parquet and Delta Lake formats.
- Understanding incremental processing with Glue Job Bookmarks.
- Monitoring ETL jobs through CloudWatch.

## Future Improvements

- Implement failed-record handling with a separate S3 location.
- Add automated Glue job scheduling.
- Improve error handling and logging.
- Implement incremental upserts for aggregated Delta data.
- Add automated pipeline monitoring.

## Technology Stack

`AWS Glue` · `Amazon S3` · `PySpark` · `Python` · `Parquet` · `Delta Lake` · `CloudWatch`

---

**Project Type:** Data Engineering | Cloud ETL Pipeline

**Cloud Platform:** Amazon Web Services (AWS)
