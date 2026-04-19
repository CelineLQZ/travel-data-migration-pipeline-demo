# AWS Manual Setup Guide

Create the following AWS resources before running the demo.
Everything is in `eu-west-1` (Ireland) unless noted.

---

## 1. S3 Bucket

1. Go to **S3 → Create bucket**
2. Name: `travel-migration-demo` (or any globally unique name)
3. Region: `eu-west-1`
4. Block all public access: **enabled** (default)
5. Create these "folders" (just upload a blank `.keep` file in each path):
   ```
   raw/hotels/
   raw/customers/
   raw/bookings/
   staging/hotels/
   staging/customers/
   staging/bookings/
   staging/errors/hotels/
   staging/errors/customers/
   staging/errors/bookings/
   scripts/           ← upload spark_jobs/*.py here before running the DAG
   ```

---

## 2. IAM User — for local scripts and Airflow

1. Go to **IAM → Users → Create user**
2. Name: `travel-demo-user`
3. Attach policy: **AmazonS3FullAccess**
   *(demo only — in production you would scope to the specific bucket)*
4. Go to **Security credentials → Create access key**
5. Use case: **Other** → Create
6. Copy the **Access key ID** and **Secret access key** into your `.env` file

---

## 3. IAM Role — for EMR Serverless

1. Go to **IAM → Roles → Create role**
2. Trusted entity type: **Custom trust policy**
3. Paste this trust policy:
   ```json
   {
     "Version": "2012-10-17",
     "Statement": [{
       "Effect": "Allow",
       "Principal": { "Service": "emr-serverless.amazonaws.com" },
       "Action": "sts:AssumeRole"
     }]
   }
   ```
4. Name: `emr-serverless-execution-role`
5. Attach these policies:
   - **AmazonS3FullAccess**
   - **AmazonEMRServerlessServicePolicy**
6. Copy the **Role ARN** → put it in `.env` as `EMR_EXECUTION_ROLE_ARN`

---

## 4. IAM Role — for Snowflake Storage Integration

> You'll update the trust policy after creating the Snowflake integration in step 6.

1. Go to **IAM → Roles → Create role**
2. Trusted entity: **AWS account** (placeholder — you'll update this later)
3. Name: `snowflake-s3-role`
4. Attach an inline policy with S3 read access to your bucket:
   ```json
   {
     "Version": "2012-10-17",
     "Statement": [{
       "Effect": "Allow",
       "Action": ["s3:GetObject", "s3:ListBucket"],
       "Resource": [
         "arn:aws:s3:::YOUR_BUCKET_NAME",
         "arn:aws:s3:::YOUR_BUCKET_NAME/*"
       ]
     }]
   }
   ```
5. Note the **Role ARN** — you'll need it in step 6

---

## 5. EMR Serverless Application

1. Go to **EMR → EMR Serverless → Create application**
2. Name: `travel-migration-demo`
3. Type: **Spark**
4. Release: **emr-6.15.0** (or latest stable 6.x)
5. Leave other settings as default → **Create**
6. Copy the **Application ID** → put it in `.env` as `EMR_APP_ID`

> Before triggering the DAG, upload the Spark scripts to S3:
> ```bash
> aws s3 cp spark_jobs/transform_hotels.py    s3://YOUR_BUCKET/scripts/
> aws s3 cp spark_jobs/transform_customers.py s3://YOUR_BUCKET/scripts/
> aws s3 cp spark_jobs/transform_bookings.py  s3://YOUR_BUCKET/scripts/
> ```

---

## 6. Snowflake Storage Integration

Run these commands in the **Snowflake Worksheet**:

```sql
-- Step 1: create the integration (replace ACCOUNT_ID and bucket name)
CREATE STORAGE INTEGRATION s3_integration
  TYPE = EXTERNAL_STAGE
  STORAGE_PROVIDER = 'S3'
  ENABLED = TRUE
  STORAGE_AWS_ROLE_ARN = 'arn:aws:iam::ACCOUNT_ID:role/snowflake-s3-role'
  STORAGE_ALLOWED_LOCATIONS = ('s3://YOUR_BUCKET_NAME/');

-- Step 2: get the IAM values Snowflake generated for you
DESC INTEGRATION s3_integration;
```

From the output, copy:
- `STORAGE_AWS_IAM_USER_ARN`  → e.g. `arn:aws:iam::1234567890:user/snowflake-user`
- `STORAGE_AWS_EXTERNAL_ID`   → e.g. `MYACCOUNT_SFCRole=...`

**Step 3**: Go back to IAM → Role `snowflake-s3-role` → **Trust relationships → Edit**

Replace the trust policy with:

```json
{
  "Version": "2012-10-17",
  "Statement": [{
    "Effect": "Allow",
    "Principal": { "AWS": "STORAGE_AWS_IAM_USER_ARN" },
    "Action": "sts:AssumeRole",
    "Condition": {
      "StringEquals": { "sts:ExternalId": "STORAGE_AWS_EXTERNAL_ID" }
    }
  }]
}
```

---

## 7. Run Order

Once all AWS resources are created:

```bash
# 1. Copy and fill in credentials
cp .env.example .env

# 2. Generate data locally (no AWS needed for this step)
python data_generator/generate_data.py

# 3. Upload Spark scripts to S3
aws s3 cp spark_jobs/ s3://YOUR_BUCKET/scripts/ --recursive

# 4. Run Snowflake DDL (in Snowflake Worksheet, in order)
#    01_raw_tables.sql → 02_staging_tables.sql → 03_analytics_table.sql → 04_error_table.sql

# 5. Start Airflow
docker-compose up airflow-init   # wait for exit 0
docker-compose up -d

# 6. Open http://localhost:8080 (admin/admin) and trigger travel_migration_demo
```
