# data-vault-medallion-architecture

Implementation of Data Vault modeling on a medallion architecture (Bronze–Silver–Gold) using PySpark and Delta Lake.


\## Project Status

\- \[ ] Step 1: GitHub Setup ← CURRENT STEP

\- \[ ] Step 2: Azure Infrastructure Setup

\- \[ ] Step 3: Sample Data Generation

\- \[ ] Step 4: Bronze Layer Implementation

\- \[ ] Step 5: Silver Layer (Data Vault)

\- \[ ] Step 6: Gold Layer (Dimensional Model)

\- \[ ] Step 7: Documentation \& Deployment



\## Technology Stack

\- Azure Databricks

\- Azure Data Lake Storage Gen2

\- Delta Lake

\- Apache Spark

\- Data Vault 2.0



\## Getting Started



```bash

git clone https://github.com/your-username/data-vault-medallion-azure.git

cd data-vault-medallion-azure




```markdown
## Azure Infrastructure Setup

### Prerequisites
- Azure CLI installed
- Azure subscription with appropriate permissions

### Quick Setup
```bash
# Run infrastructure setup
cd scripts/infrastructure
./azure-setup.sh

# Validate setup
./validate-setup.ps1





```markdown
## Sample Data Generation

### Generate Sample Data
```bash
# Install dependencies
pip install -r scripts/data-generation/requirements.txt

# Generate data
cd scripts/data-generation
python generate-sample-data.py

# Validate data
python validate-data.py



Add bronze layer section:

```markdown
## Bronze Layer Implementation

### Data Ingestion
```python
# Run bronze layer ingestion
cd notebooks/1_bronze
databricks notebook run 01_bronze_ingestion.py


 

Add Silver Layer section:

```markdown
## Silver Layer - Data Vault Implementation

### Data Vault Modeling
```python
# Run Data Vault implementation
cd notebooks/2_silver
databricks notebook run 01_data_vault_modeling.py


Data Vault Components
🔑 Hub Tables: 3 tables (Customer, Product, Order business keys)

🔗 Link Tables: 2 tables (Customer-Order, Order-Product relationships)

📊 Satellite Tables: 3 tables (Attributes with history)


Features
✅ Data Vault 2.0 methodology implemented

✅ SHA-256 hash keys for business key identification

✅ Historical data tracking in satellites

✅ Scalable relationship modeling

✅ Audit trail through metadata


## Step 5.4: Run the Data Vault Implementation

### Execute in Databricks:
1. **Upload** the silver layer notebook to Databricks
2. **Run** `01_data_vault_modeling.py` notebook
3. **Verify** Data Vault tables are created in silver container

 
