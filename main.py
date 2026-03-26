# !python --version => Python 3.12.12
import json
from datetime import datetime, timezone
import apache_beam as beam
from apache_beam.options.pipeline_options import (
    PipelineOptions,
    StandardOptions,
    SetupOptions,
    GoogleCloudOptions
)
from apache_beam.io.gcp.internal.clients import bigquery
from datetime import datetime, timezone

PROJECT_ID = "project-5dd8f491-cc9c-4f1e-951"
REGION = "asia-south2"
SUBSCRIPTION = "projects/project-5dd8f491-cc9c-4f1e-951/subscriptions/data_engineering_subscription"
BQ_TABLE = "project-5dd8f491-cc9c-4f1e-951:assignment_2.waqi_hyd_bronze"


class ParseWAQI(beam.DoFn):
    def process(self, message):
        msg = json.loads(message.decode("utf-8"))
        data = msg["raw_payload"]["data"]
        iaqi = data.get("iaqi", {})
        city_info = data.get("city", {})

        # Parse event_time and normalize to UTC
        event_time = datetime.fromisoformat(msg["event_time"])
        event_time_utc = event_time.astimezone(timezone.utc)

        def safe_float(val):
            try:
                return float(val)
            except (TypeError, ValueError):
                return None

        def safe_int(val):
            try:
                return int(val)
            except (TypeError, ValueError):
                return None

        yield {
            "event_date": event_time_utc.date().isoformat(),
            "city": str(msg.get("city", "")),
            "station_name": str(city_info.get("name", "")),
            "lat": safe_float(city_info.get("geo", [None, None])[0]),
            "lon": safe_float(city_info.get("geo", [None, None])[1]),
            "aqi": safe_int(data.get("aqi")),
            "dominant_pollutant": str(data.get("dominentpol", "")),
            "pm25": safe_float(iaqi.get("pm25", {}).get("v")),
            "pm10": safe_float(iaqi.get("pm10", {}).get("v")),
            "co": safe_float(iaqi.get("co", {}).get("v")),
            "no2": safe_float(iaqi.get("no2", {}).get("v")),
            "so2": safe_float(iaqi.get("so2", {}).get("v")),
            "temperature": safe_float(iaqi.get("t", {}).get("v")),
            "humidity": safe_float(iaqi.get("h", {}).get("v")),
            "wind": safe_float(iaqi.get("w", {}).get("v")),
            "event_time": event_time_utc.strftime("%Y-%m-%d %H:%M:%S"),
            "bq_load_time": datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")
        }



# BigQuery schema
table_schema = bigquery.TableSchema()

def add_field(name, type_, mode="NULLABLE"):
    field = bigquery.TableFieldSchema()
    field.name = name
    field.type = type_
    field.mode = mode
    table_schema.fields.append(field)

add_field("event_date", "DATE")
add_field("city", "STRING")
add_field("station_name", "STRING")
add_field("lat", "FLOAT")
add_field("lon", "FLOAT")
add_field("aqi", "INTEGER")
add_field("dominant_pollutant", "STRING")
add_field("pm25", "FLOAT")
add_field("pm10", "FLOAT")
add_field("co", "FLOAT")
add_field("no2", "FLOAT")
add_field("so2", "FLOAT")
add_field("temperature", "FLOAT")
add_field("humidity", "FLOAT")
add_field("wind", "FLOAT")
add_field("event_time", "TIMESTAMP")
add_field("bq_load_time", "TIMESTAMP")


def run():
    options = PipelineOptions()
    gcloud_options = options.view_as(GoogleCloudOptions)
    gcloud_options.project = PROJECT_ID
    gcloud_options.region = REGION
    gcloud_options.job_name = "waqi-dataflow-job-new"
    gcloud_options.staging_location = "gs://temp_test_bucket_11022026/staging"
    gcloud_options.temp_location = "gs://temp_test_bucket_11022026/temp"

    options.view_as(StandardOptions).streaming = True
    options.view_as(SetupOptions).save_main_session = True
    options.view_as(StandardOptions).runner = "DataflowRunner"  # switch to DirectRunner/DataflowRunner for production

    with beam.Pipeline(options=options) as p:
        (
            p
            | "Read from PubSub" >> beam.io.ReadFromPubSub(subscription=SUBSCRIPTION)
            | "Parse WAQI Payload" >> beam.ParDo(ParseWAQI())
            | "Write to BigQuery" >> beam.io.WriteToBigQuery(
                table=BQ_TABLE,
                schema=table_schema,
                write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND,
                create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED
            )
        )


if __name__ == "__main__":
    run()



"""
Integration Summary
Cloud Scheduler → orchestrates timing.
Cloud Function → event-driven bridge.
Pub/Sub → streams messages.
Dataflow (Beam) → processes and structures data.
BigQuery → stores analytics-ready tables.
Terraform → automates infrastructure.
"""

# Flex Templates let you run Dataflow jobs directly from container images stored in Artifact Registry, making pipelines portable and production-ready.

# ReadFromPubSub(topic) vs ReadFromPubSub(subscription)
"""
In Apache Beam, ReadFromPubSub(topic=...) reads directly from a Pub/Sub topic, while ReadFromPubSub(subscription=...) consumes messages 
from a specific subscription. The key difference is that reading from a topic creates an implicit subscription managed by Dataflow, 
whereas reading from a subscription uses an explicit subscription you control.

For production workloads, always prefer ReadFromPubSub(subscription=...) because it gives you control over message retention and delivery 
guarantees. Use topic only for quick experiments or temporary pipelines.
"""

"""
[Cloud Scheduler]
         ↓ (HTTP)
[Cloud Function – fetch API]
         ↓ (publish)
[Pub/Sub Topic → Subscription]
         ↓ (stream)
[Dataflow (Apache Beam)]
         ↓ (write)
[BigQuery – Bronze Layer]
         ↓ (SQL transform via dbt)
[BigQuery – Silver Layer]
         ↓ (aggregations)
[BigQuery – Gold Layer]
         ↓
(BI/Analytics tools later)
"""

"""
source .venv/bin/activate

cd data\INGESTION\WORKING_CODE\Dataflow\Notebooks\project_2\GCP_End_to_End_Pipeline-main\dataflow

gcloud artifacts repositories create dataflow-docker-repo \
    --repository-format=docker \
    --location=asia-south2 \
    --description="Docker repo for dataflow-docker-repo images" \
    --project=project-5dd8f491-cc9c-4f1e-951

gcloud dataflow flex-template build \
  gs://temp_test_bucket_25032026/templates/waqi_flex.json \
  --sdk-language PYTHON \
  --flex-template-base-image PYTHON3 \
  --py-path "." \
  --image-gcr-path asia-south2-docker.pkg.dev/project-5dd8f491-cc9c-4f1e-951/dataflow-docker-repo/dataflow-docker-repo-waqi:1.0.0 \
  --metadata-file metadata.json \
  --env FLEX_TEMPLATE_PYTHON_PY_FILE=main.py \
  --env FLEX_TEMPLATE_PYTHON_REQUIREMENTS_FILE=requirements.txt

gcloud dataflow flex-template run waqi-dataflow-run \
  --template-file-gcs-location gs://temp_test_bucket_25032026/templates/waqi_flex.json \
  --region asia-south2
"""

# Find your project number (different from project ID):
"""
gcloud projects describe project-5dd8f491-cc9c-4f1e-951 --format="value(projectNumber)"
"""

# You must grant roles/pubsub.subscriber to the Dataflow worker service account on your subscription. 
"""
gcloud pubsub subscriptions add-iam-policy-binding data_engineering_subscription \
  --member="serviceAccount:service-306164924329@dataflow-service-producer-prod.iam.gserviceaccount.com" \
  --role="roles/pubsub.subscriber"

gcloud artifacts repositories add-iam-policy-binding dataflow-docker-repo \
  --location=asia-south2 \
  --member="serviceAccount:service-306164924329@dataflow-service-producer-prod.iam.gserviceaccount.com" \
  --role="roles/artifactregistry.reader"

gcloud storage buckets add-iam-policy-binding temp_test_bucket_25032026 \
  --member="serviceAccount:service-306164924329@dataflow-service-producer-prod.iam.gserviceaccount.com" \
  --role="roles/storage.objectViewer"

gcloud bigquery datasets add-iam-policy-binding assignment_2 \
  --member="serviceAccount:service-306164924329@dataflow-service-producer-prod.iam.gserviceaccount.com" \
  --role="roles/bigquery.dataEditor"
"""
# Can I start stopped dataflow job ?
"""
No — once a Dataflow job is stopped, it cannot be restarted.
"""

# does each services have default service account
# # You must grant roles/pubsub.subscriber to the Dataflow worker service account on your subscription. 
# cloud build and so on

"""
Dataflow: Default worker service account:
service-<PROJECT_NUMBER>@dataflow-service-producer-prod.iam.gserviceaccount.com
Needs roles like roles/pubsub.subscriber, roles/bigquery.dataEditor, roles/storage.objectViewer, 
roles/artifactregistry.reader.


Cloud Build: Default service account:
<PROJECT_NUMBER>@cloudbuild.gserviceaccount.com
Needs roles like roles/storage.admin (to push artifacts), roles/artifactregistry.writer (to push images), etc.


Compute Engine: Default service account:
<PROJECT_NUMBER>-compute@developer.gserviceaccount.com
Often used by VMs unless you attach a custom service account.


App Engine: Default service account:
<PROJECT_ID>@appspot.gserviceaccount.com
"""

"""
When you run a job (Dataflow, Cloud Build, etc.), the service uses its default account unless you override it. 
If that account doesn’t have the right IAM roles, the job will fail silently or hang (like your Dataflow job 
not consuming Pub/Sub messages).

Yes, each service has a default service account. You must grant the right IAM roles to those accounts 
(or use a custom service account) so the jobs can access Pub/Sub, BigQuery, GCS, Artifact Registry, etc.
"""

"""
# Allow Cloud Build service account to use APIs
gcloud projects add-iam-policy-binding project-5dd8f491-cc9c-4f1e-951 \
  --member="serviceAccount:306164924329@cloudbuild.gserviceaccount.com" \
  --role="roles/serviceusage.serviceUsageConsumer"

# Allow Cloud Build service account to write logs to its bucket
gcloud storage buckets add-iam-policy-binding gs://project-5dd8f491-cc9c-4f1e-951_cloudbuild \
  --member="serviceAccount:306164924329@cloudbuild.gserviceaccount.com" \
  --role="roles/storage.objectAdmin"

"""

# To make your CI/CD pipeline fully functional, you’ll also want to grant this Cloud Build service account the roles 
# it needs for the build steps:

# Artifact Registry:
"""
gcloud artifacts repositories add-iam-policy-binding dataflow-docker-repo \
  --location=asia-south2 \
  --member="serviceAccount:306164924329@cloudbuild.gserviceaccount.com" \
  --role="roles/artifactregistry.writer"
"""

# GCS bucket (for templates)::
"""
gcloud storage buckets add-iam-policy-binding temp_test_bucket_25032026 \
  --member="serviceAccount:306164924329@cloudbuild.gserviceaccount.com" \
  --role="roles/storage.admin"
"""

# Dataflow::
"""
gcloud projects add-iam-policy-binding project-5dd8f491-cc9c-4f1e-951 \
  --member="serviceAccount:306164924329@cloudbuild.gserviceaccount.com" \
  --role="roles/dataflow.admin"
"""

"""
gcloud builds submit \
  --config=cloudbuild.yaml \
  --service-account=306164924329@cloudbuild.gserviceaccount.com \
  --logs-bucket=gs://temp_test_bucket_25032026
"""

"""
gcloud projects describe project-5dd8f491-cc9c-4f1e-951 --format="value(projectNumber)"


gcloud projects add-iam-policy-binding project-5dd8f491-cc9c-4f1e-951 \
--member="serviceAccount:306164924329@cloudbuild.gserviceaccount.com" \
--role="roles/serviceusage.serviceUsageConsumer"

gsutil iam ch \
serviceAccount:306164924329@cloudbuild.gserviceaccount.com:roles/storage.admin \
gs://project-5dd8f491-cc9c-4f1e-951_cloudbuild



gsutil iam ch \
serviceAccount:306164924329@cloudbuild.gserviceaccount.com:roles/storage.admin \
gs://temp_test_bucket_25032026


gcloud services enable \
cloudbuild.googleapis.com \
dataflow.googleapis.com \
artifactregistry.googleapis.com \
storage.googleapis.com \
compute.googleapis.com \
serviceusage.googleapis.com
"""


"""
PROJECT_ID=project-5dd8f491-cc9c-4f1e-951
PROJECT_NUMBER=$(gcloud projects describe $PROJECT_ID --format="value(projectNumber)")

gcloud projects add-iam-policy-binding $PROJECT_ID \
--member="serviceAccount:${PROJECT_NUMBER}@cloudbuild.gserviceaccount.com" \
--role="roles/editor"

gcloud projects add-iam-policy-binding $PROJECT_ID \
--member="serviceAccount:${PROJECT_NUMBER}@cloudbuild.gserviceaccount.com" \
--role="roles/serviceusage.serviceUsageConsumer"

gsutil iam ch \
serviceAccount:${PROJECT_NUMBER}@cloudbuild.gserviceaccount.com:roles/storage.admin \
gs://temp_test_bucket_25032026

gsutil iam ch \
serviceAccount:${PROJECT_NUMBER}@cloudbuild.gserviceaccount.com:roles/storage.admin \
gs://project-5dd8f491-cc9c-4f1e-951_cloudbuild
"""

"""
ERROR: (gcloud.dataflow.flex-template.build) The user is forbidden from accessing the bucket 
[project-5dd8f491-cc9c-4f1e-951_cloudbuild]. Please check your organization's policy or if the user has the 
"serviceusage.services.use" permission. Giving the user a role with this permission such as Service Usage Admin
 may fix this issue. Alternatively, use the --no-source option and access your source code via a different method.
Finished Step #2
ERROR
ERROR: build step 2 "gcr.io/google.com/cloudsdktool/cloud-sdk" failed: step exited with non-zero status: 1
"""