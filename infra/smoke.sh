#!/usr/bin/env bash
# Runs only after a real apply. Incurs small cloud compute charges to trial credits.
set -euo pipefail
cd "$(dirname "$0")/.."
TF="${TERRAFORM:-terraform}"
PY="${PYTHON:-python3}"
command -v gcloud >/dev/null
command -v "$TF" >/dev/null
project=$("$TF" -chdir=infra output -raw project_id)
region=$("$TF" -chdir=infra output -raw region)
bucket=$("$TF" -chdir=infra output -raw data_bucket)
scratch=$("$TF" -chdir=infra output -raw scratch_bucket)
subnet=$("$TF" -chdir=infra output -raw spark_subnet)
worker=$("$TF" -chdir=infra output -json service_accounts | "$PY" -c 'import json,sys; print(json.load(sys.stdin)["spark"])')
batch="aq-s02-$(date -u +%Y%m%d%H%M%S)"

gcloud storage buckets describe "gs://$bucket" --project="$project"
gcloud sql instances describe aq-operational --project="$project" --format='value(state,connectionName)'
gcloud artifacts repositories describe air-quality --location="$region" --project="$project"
for secret in aq-purpleair-read-key aq-airnow-api-key aq-snowflake-creds; do
  gcloud secrets describe "$secret" --project="$project" --format='value(name)'
done
for job in epa-download epa-stage purpleair-poll airnow-collect live-sync; do
  image=$(gcloud run jobs describe "aq-$job" --region="$region" --project="$project" \
    --format='value(spec.template.spec.template.spec.containers[0].image)')
  if [[ "$image" == "docker.io/library/python:3.15-slim" ]]; then
    gcloud run jobs execute "aq-$job" --wait --region="$region" --project="$project"
  else
    printf 'Skipping deployed collector aq-%s; use its segment verification.\n' "$job"
  fi
done
gcloud scheduler jobs list --location="$region" --project="$project" --format='table(name,state,schedule)'

gcloud dataproc batches submit pyspark infra/smoke_spark.py \
  --project="$project" --region="$region" --batch="$batch" --version=2.2 \
  --service-account="$worker" --subnet="$subnet" --tags=aq-spark \
  --staging-bucket="$scratch" --deps-bucket="gs://$scratch" --ttl=10m \
  --properties=spark.dynamicAllocation.enabled=false,spark.executor.instances=2,spark.executor.cores=4,spark.driver.cores=4 \
  -- "gs://$bucket/checks/s02/$batch"
gcloud dataproc batches describe "$batch" --project="$project" --region="$region" \
  --format='json(name,state,stateMessage,runtimeInfo,createTime)'
