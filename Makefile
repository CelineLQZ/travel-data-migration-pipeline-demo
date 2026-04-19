AWS_REGION   = eu-north-1
BUCKET       = travel-industry-migration-demo-celineli
SPARK_SCRIPTS = transform_hotels.py transform_customers.py transform_bookings.py

# -----------------------------------------------------------------------
# Infrastructure
# -----------------------------------------------------------------------

.PHONY: infra-init
infra-init:
	cd terraform && terraform init

.PHONY: infra-apply
infra-apply:
	cd terraform && terraform apply
	$(MAKE) env-sync

.PHONY: infra-destroy
infra-destroy:
	cd terraform && terraform destroy

# Sync Terraform outputs → .env
.PHONY: env-sync
env-sync:
	@echo "Syncing Terraform outputs to .env..."
	@KEY=$$(cd terraform && terraform output -raw aws_access_key_id) && \
	 SECRET=$$(cd terraform && terraform output -raw aws_secret_access_key) && \
	 EMR_APP=$$(cd terraform && terraform output -raw emr_app_id) && \
	 EMR_ROLE=$$(cd terraform && terraform output -raw emr_execution_role_arn) && \
	 sed -i '' \
	   -e "s|AWS_ACCESS_KEY_ID=.*|AWS_ACCESS_KEY_ID=$$KEY|" \
	   -e "s|AWS_SECRET_ACCESS_KEY=.*|AWS_SECRET_ACCESS_KEY=$$SECRET|" \
	   -e "s|EMR_APP_ID=.*|EMR_APP_ID=$$EMR_APP|" \
	   -e "s|EMR_EXECUTION_ROLE_ARN=.*|EMR_EXECUTION_ROLE_ARN=$$EMR_ROLE|" \
	   .env
	@echo "Done. .env updated."

# -----------------------------------------------------------------------
# Data pipeline
# -----------------------------------------------------------------------

.PHONY: generate-data
generate-data:
	python3 data_generator/generate_data.py

.PHONY: upload-scripts
upload-scripts:
	@for script in $(SPARK_SCRIPTS); do \
	  aws s3 cp spark_jobs/$$script s3://$(BUCKET)/scripts/ --region $(AWS_REGION); \
	done

.PHONY: upload-data
upload-data:
	aws s3 cp local/hdfs_simulation/hotels/hotels.parquet s3://$(BUCKET)/raw/hotels/ --region $(AWS_REGION)
	aws s3 cp local/hdfs_simulation/customers/customers.parquet s3://$(BUCKET)/raw/customers/ --region $(AWS_REGION)
	aws s3 cp local/hdfs_simulation/bookings/bookings.parquet s3://$(BUCKET)/raw/bookings/ --region $(AWS_REGION)

# -----------------------------------------------------------------------
# Airflow
# -----------------------------------------------------------------------

.PHONY: airflow-init
airflow-init:
	docker-compose up airflow-init

.PHONY: airflow-start
airflow-start:
	docker-compose up -d

.PHONY: airflow-stop
airflow-stop:
	docker-compose down

.PHONY: airflow-restart
airflow-restart:
	docker-compose down
	docker-compose up -d

# -----------------------------------------------------------------------
# Full setup from scratch
# -----------------------------------------------------------------------

.PHONY: setup
setup: infra-apply upload-scripts generate-data upload-data airflow-init airflow-start
	@echo "Setup complete. Open http://localhost:8080 to trigger the DAG."
