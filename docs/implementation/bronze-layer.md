\# Bronze Layer Implementation Guide



\## Overview

The Bronze Layer is the first layer in the Medallion architecture where raw data is ingested with metadata.



\## Architecture



Source Data → Azure Storage → Bronze Layer (Delta Format) → Data Validation





\## Data Flow

1\. Read CSV files from `sample-data` container

2\. Add metadata columns for auditability

3\. Write to Delta format in `bronze` container

4\. Perform data quality checks



\## Metadata Columns Added

\- `ingestion\_timestamp`: When the data was ingested

\- `source\_system`: Source system identifier ("sample\_data")

\- `source\_file`: Original file name

\- `load\_id`: Unique identifier for the load operation



\## Tables Ingested

1\. \*\*customers\*\*: Customer information

2\. \*\*products\*\*: Product catalog

3\. \*\*orders\*\*: Order transactions

4\. \*\*order\_items\*\*: Order line items



\## Data Quality Checks

\- Null value validation in key columns

\- Duplicate record detection

\- Record count validation

\- Data type validation



\## Execution

Run the bronze layer notebook:

```python

\# Run complete bronze ingestion

ingest\_all\_tables()



\# Verify data

verify\_bronze\_layer()



\# Check quality

perform\_data\_quality\_checks()

