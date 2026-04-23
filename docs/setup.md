# 🛠️ Setup & Installation Guide

Complete guide to setting up and running Project Voltstream in your Databricks environment.

---

## Prerequisites

* **Databricks Workspace** (AWS, Azure, or GCP)
* **Databricks Runtime**: DBR 13.3 LTS or higher
* **Python**: 3.10+
* **Cluster Configuration**:
  * Single node or multi-node cluster
  * Recommended: Standard_DS3_v2 or equivalent
  * Unity Catalog enabled (optional but recommended)

---

## 1️⃣ API Keys Configuration

This project requires API keys from two external data sources:

### Open Charge Map API
1. Visit [Open Charge Map](https://openchargemap.org/site/develop/api)
2. Register for a free API key
3. Note your API key (format: `xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx`)

### OpenWeatherMap API
1. Visit [OpenWeatherMap](https://openweathermap.org/api)
2. Sign up for a free account
3. Generate an API key from your account dashboard
4. Note your API key (format: `xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx`)

---

## 2️⃣ Databricks Secrets Setup

Store API keys securely using Databricks secrets:

```bash
# Install Databricks CLI (if not already installed)
pip install databricks-cli

# Configure Databricks CLI
databricks configure --token

# Create a secret scope
databricks secrets create-scope --scope project_voltstream

# Add API keys to secrets
databricks secrets put --scope project_voltstream --key open_charge_map_api_key
databricks secrets put --scope project_voltstream --key openweathermap_api_key
```

**Alternative: Using Databricks UI**
1. Go to your Databricks workspace
2. Navigate to Settings → Developer → Secrets
3. Create a new secret scope: `project_voltstream`
4. Add secrets:
   * `open_charge_map_api_key`
   * `openweathermap_api_key`

---

## 3️⃣ Clone Repository to Databricks

### Option A: Using Databricks Repos (Recommended)
1. In Databricks workspace, click **Repos** in left sidebar
2. Click **Add Repo**
3. Enter GitHub URL: `https://github.com/YOUR_USERNAME/project_voltstream`
4. Click **Create Repo**

### Option B: Manual Upload
1. Clone repository locally:
   ```bash
   git clone https://github.com/YOUR_USERNAME/project_voltstream.git
   ```
2. Upload to Databricks workspace:
   ```bash
   databricks workspace import-dir ./project_voltstream /Workspace/Users/YOUR_EMAIL/project_voltstream
   ```

---

## 4️⃣ Install Dependencies

Create a notebook in your Databricks workspace and run:

```python
%pip install -r requirements.txt
```

Or install packages directly:

```python
%pip install databricks-sdk requests
```

---

## 5️⃣ Create Unity Catalog Schemas

Run the following in a Databricks notebook or SQL editor:

```sql
-- Create catalogs (if they don't exist)
CREATE CATALOG IF NOT EXISTS bronze_dev;
CREATE CATALOG IF NOT EXISTS silver_dev;
CREATE CATALOG IF NOT EXISTS gold_dev;

-- Create schemas
CREATE SCHEMA IF NOT EXISTS bronze_dev.electrovolt;
CREATE SCHEMA IF NOT EXISTS silver_dev.electrovolt;
CREATE SCHEMA IF NOT EXISTS gold_dev.electrovolt;

-- Verify creation
SHOW SCHEMAS IN bronze_dev;
SHOW SCHEMAS IN silver_dev;
SHOW SCHEMAS IN gold_dev;
```

---

## 6️⃣ Configure Pipeline Parameters

Update the bounding box coordinates in your configuration (if needed):

**File**: `utils/bronze.py` or configuration notebook

```python
# New York City + Northern New Jersey bounding box
BOUNDING_BOX = {
    "min_latitude": 40.477399,
    "max_latitude": 41.287200,
    "min_longitude": -74.259090,
    "max_longitude": -73.700181
}
```

---

## 7️⃣ Run Initial Ingestion

### Manual Execution
1. Open the pipeline notebooks in order:
   * `notebooks/01_bronze_ingestion.py`
   * `notebooks/02_silver_transformation.py`
   * `notebooks/03_gold_aggregation.py`

2. Run each notebook sequentially or use "Run All"

### Workflow Execution
1. Navigate to **Workflows** in Databricks
2. Import the workflow definition:
   ```bash
   databricks jobs create --json-file workflows/voltstream_pipeline.json
   ```
3. Click **Run Now** to execute the entire pipeline

---

## 8️⃣ Schedule the Pipeline

Set up the 5-minute incremental refresh:

1. Go to **Workflows** → **Jobs**
2. Find "Voltstream Pipeline" job
3. Click **Edit Schedule**
4. Set schedule:
   * **Trigger**: Scheduled
   * **Interval**: Every 5 minutes
   * **Cluster**: Use existing cluster or create new

**Cron Expression**: `0 */5 * * * ?` (every 5 minutes)

---

## 9️⃣ Verify Installation

Run the health check to validate everything is working:

```python
# Run health check script
%run ./health_check/health_check.py
```

Expected output:
```
✓ All health checks passed!
- Bronze tables: 2,216 rows
- Silver tables: 2,216 stations, 53 weather zones
- Gold tables: station_facts, station_dim, weather_dim
- Data freshness: <2 hours
```

---

## 🔟 Access the Dashboard

1. Open Power BI Desktop
2. Connect to Databricks:
   * **Get Data** → **Databricks**
   * Enter workspace URL and personal access token
3. Import views from `gold_dev.electrovolt`:
   * `station_facts`
   * `station_dim`
   * `weather_dim`
4. Build visualizations or import the provided `.pbix` file

---

## 🚀 Quick Start Commands

```bash
# 1. Clone repository
git clone https://github.com/YOUR_USERNAME/project_voltstream.git
cd project_voltstream

# 2. Set up Databricks secrets
databricks secrets create-scope --scope project_voltstream
databricks secrets put --scope project_voltstream --key open_charge_map_api_key
databricks secrets put --scope project_voltstream --key openweathermap_api_key

# 3. Upload to Databricks
databricks workspace import-dir . /Workspace/Users/YOUR_EMAIL/project_voltstream

# 4. Create catalogs (run in Databricks SQL)
CREATE CATALOG IF NOT EXISTS bronze_dev;
CREATE CATALOG IF NOT EXISTS silver_dev;
CREATE CATALOG IF NOT EXISTS gold_dev;

# 5. Run pipeline (from Databricks notebook)
%run ./notebooks/01_bronze_ingestion
%run ./notebooks/02_silver_transformation
%run ./notebooks/03_gold_aggregation

# 6. Verify
%run ./health_check/health_check.py
```

---

## 📝 Configuration Reference

| Parameter | Location | Default Value | Description |
|-----------|----------|---------------|-------------|
| `OPEN_CHARGE_MAP_API_KEY` | Databricks Secrets | (required) | API key for EV station data |
| `OPENWEATHERMAP_API_KEY` | Databricks Secrets | (required) | API key for weather data |
| `BOUNDING_BOX` | `utils/bronze.py` | NYC + NJ | Geographic area for data collection |
| `CATALOG_PREFIX` | Workflow config | `*_dev` | Unity Catalog environment prefix |
| `SCHEDULE_INTERVAL` | Workflow | 5 minutes | Ingestion frequency |

---

## 🆘 Troubleshooting

### Issue: API Rate Limits
**Error**: `429 Too Many Requests`  
**Solution**: 
- Reduce ingestion frequency from 5 to 15 minutes
- Implement exponential backoff (already included in `utils/shared.py`)

### Issue: Secret Not Found
**Error**: `Secret does not exist with scope: project_voltstream`  
**Solution**:
- Verify secret scope exists: `databricks secrets list-scopes`
- Create scope if missing: `databricks secrets create-scope --scope project_voltstream`

### Issue: Unity Catalog Permissions
**Error**: `PERMISSION_DENIED: User does not have CREATE_CATALOG on System`  
**Solution**:
- Contact workspace admin to grant catalog creation permissions
- Or use existing catalogs and update code to reference them

### Issue: Cluster Timeout
**Error**: `Cluster {cluster_id} does not exist or has been terminated`  
**Solution**:
- Ensure cluster auto-termination is set to >60 minutes
- Use serverless compute for Workflows (recommended)

### Issue: Health Check Fails
**Error**: Various health check failures  
**Solution**:
- Check data freshness: `SELECT MAX(ingest_timestamp) FROM gold_dev.electrovolt.station_facts`
- Verify pipeline completed: Check Workflow run history
- Review logs in failed tasks

---

## 🔒 Security Best Practices

* ✅ Always use Databricks Secrets for API keys (never hardcode)
* ✅ Use service principals for production deployments
* ✅ Enable Unity Catalog for fine-grained access control
* ✅ Rotate API keys every 90 days
* ✅ Use separate secret scopes for dev/staging/prod environments
* ✅ Audit workspace access logs regularly

---

## 📞 Support

For issues with setup:
1. Check [Troubleshooting](#-troubleshooting) section above
2. Review Databricks job run logs
3. Verify API keys are valid and have not expired
4. Ensure Unity Catalog permissions are correctly configured

---

[← Back to README](../README.md)
