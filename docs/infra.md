# S02: Google Cloud infrastructure

Status: deployed and verified in `final-team-514205` on November 8, 2026
(verification timestamps are stored in UTC). All 78 managed resources are in
place, and the final Terraform plan exited 0 with no changes. The supplied
project and billing account IDs are saved in ignored `infra/terraform.tfvars`.
The owner confirmed available trial credits and a January 6, 2027 expiry.
The non-secret connection settings are also saved in ignored `.env`.
Local validation uses Terraform 1.13.5, Google provider 7.46.2, and four
simulated-provider tests. Cloud results are recorded in
[`infra_evidence.json`](infra_evidence.json); the secret payloads and actual
collectors remain work for subsequent segments.

S06 update (November 9 UTC): the EPA download job now runs its pinned
application image with 1 CPU, 1 GiB RAM, a two-hour timeout, and one retry.
Terraform additionally manages the Cloud Build API and the `aq-build` identity
with scoped source-read, repository-write, and logging grants (five added
resources). The SQL schema and worker table grants are applied. Six simulated
Terraform tests cover the infrastructure and S06 job configuration. See
[`epa_download.md`](epa_download.md) for the data run and verification evidence.
The S02 smoke script now skips jobs whose placeholder image has been replaced.
S06's final real Terraform plan is clean, and its Python 3.15 reconciliation
verified all 233 raw ZIP generations against SQL state/history with zero
mismatches. The remaining placeholder images also match the workspace's
configured Python 3.15 runtime; schedules remain paused.

## Resources and ownership

Default region is `us-central1`. Terraform manages resources in an **existing**
project. It neither creates a billing account nor changes the project's billing
association. A precondition checks that the supplied billing account matches
the project's existing association. `trial_confirmed` defaults to false because
the course prohibits upgrading the account to paid billing. The billing API's
enabled flag does not establish the remaining credits or trial expiry; confirm
those in the Billing console. Check availability through December 11, 2026 and
the team's rehearsal/recording dates. [Google trial terms](https://docs.cloud.google.com/free/docs/free-cloud-features)

| Resource | Name / configuration | Purpose |
| --- | --- | --- |
| Data bucket | `<project>-air-quality`, or `bucket_name` | S01 raw/staging/curated paths; versioning, uniform access, public access prevention; nonempty destruction refused |
| Scratch bucket | `<project>-aq-spark-scratch` | Spark uploads, logs, temporary files; objects expire after 7 days |
| Six service accounts | `aq-epa`, `aq-purpleair`, `aq-airnow`, `aq-sync`, `aq-spark`, `aq-scheduler` | Separate worker and scheduler identities; no downloaded keys |
| Three empty secrets | `aq-purpleair-read-key`, `aq-airnow-api-key`, `aq-snowflake-creds` | Explicit `us-central1` replicas to comply with the project's location policy; no payload in Terraform state |
| Cloud SQL | `aq-operational`, PostgreSQL 16 Enterprise, `db-f1-micro`, zonal | Smallest shared-core operational instance; 10 GiB SSD with automatic growth off |
| Database and four IAM users | `air_quality`; one user per non-Spark worker | S05 tables and database grants applied during S06 |
| Artifact Registry | `air-quality`, Docker format | Future collector/app images; empty at S02 |
| Five Cloud Run jobs | `aq-epa-download`, `aq-epa-stage`, `aq-purpleair-poll`, `aq-airnow-collect`, `aq-live-sync` | EPA downloader uses the S06 configuration above; other jobs retain their S02 defaults until their owning segments deploy them |
| Build service account and API | `aq-build`, Cloud Build | Reads scratch `build-source/`, pushes to `air-quality`, writes logs; no source-data or SQL access |
| Three Scheduler jobs | PurpleAir every 2 min, AirNow minute 40, sync every 5 min | All paused; OAuth invocation of the corresponding Run job |
| Spark VPC, subnet, firewall | `aq-spark`, `10.42.0.0/24` | Private Google Access; internal traffic to tagged Spark workers only |
| Two custom IAM roles | `airQualitySparkWorker`, `airQualitySparkSubmitter` | Worker control-plane permissions and narrowly scoped batch submission |
| Project-filtered billing budget | $50 USD/month by default | Alerts at 50%, 90%, 100% of gross spend before credits; default billing administrators/users receive email |
| API enablement | Service Usage, Resource Manager, IAM, IAM Credentials, Storage, Secret Manager, SQL Admin, Artifact Registry, Run, Scheduler, Dataproc, Compute, Logging, Monitoring, Cloud Billing, Billing Budgets | API enablement is retained on destroy; no extra workloads created |

Bucket versioning and the default soft-delete retention consume storage for
older/deleted objects. Raw versions are retained for reconciliation; only the
scratch bucket has automatic deletion. Copy useful Spark driver logs into
durable project evidence before scratch expiry. Terraform state remains local
and ignored; one designated operator should preserve it securely and serialize
applies until the team adopts a shared remote backend.

## Access and connections

Object writes are restricted by bucket IAM conditions: EPA may modify
`raw/epa/`, `raw/aqs/`, `staging/epa/`; PurpleAir and AirNow their own raw
prefixes; sync `staging/live/`; Spark `curated/epa_hourly/` and `checks/s02/`.
Spark can also read `staging/epa/` and manage its scratch objects. Bucket-wide
listing exposes object names, since GCS listing cannot be filtered by object
IAM conditions. It does not permit reading objects outside allowed prefixes.

The default Dataproc Worker role contains broad storage permissions. The custom
role copies its non-storage permissions and grants storage access separately
on the two buckets. Review permission changes in each real Terraform plan.
The Spark worker also has subnet network access and bucket metadata/listing
access. Google-managed service agents retain their service-specific roles.
[Dataproc service account guidance](https://docs.cloud.google.com/managed-spark/docs/concepts/configuring-clusters/service-accounts)

Cloud SQL has a public endpoint with no authorized networks and enforced
connector access. The four worker identities get Cloud SQL client and IAM
login roles and corresponding database users. Use the Cloud SQL Python
Connector with automatic IAM authentication in S05; the mounted `/cloudsql`
socket alone does not provide a database login token or SQL privileges. S05
must bootstrap an administrator securely and grant table access to worker
users; no administrator password is created or stored here. Public source
collectors use Cloud Run's normal internet egress. Spark has no NAT: package
dependencies must be staged in GCS or bundled instead of fetched from PyPI.
[Spark network requirements](https://docs.cloud.google.com/managed-spark/docs/concepts/network-serverless)

Only PurpleAir can read its key, AirNow its key, and sync the Snowflake secret.
The public AirNow file collector does not currently need an API key; its secret
is reserved for API use. Add secret versions in Secret Manager or with
`gcloud secrets versions add SECRET_ID --data-file=/secure/path/to/value`.
Then reference a specific secret version in the owning job's configuration.
The placeholder jobs do not mount secrets, so they can start before keys exist.

## Deploy and verify

Use [Terraform](https://developer.hashicorp.com/terraform/install) and the
[Google Cloud CLI](https://docs.cloud.google.com/sdk/docs/install), or a Cloud
Shell workspace with this repository. Authenticate with `gcloud auth login`
for CLI commands and `gcloud auth application-default login` for Terraform.
The latter stores credentials outside the repository; never send tokens in chat.

The provisioning identity needs API enablement and administration of the
resources above: Service Usage Admin, Project IAM Admin, Role Admin, Service
Account Admin and Service Account User, Compute Network/Security Admin,
Storage Admin, Cloud SQL Admin, Secret Manager Admin, Artifact Registry Admin,
Cloud Run Admin, and Cloud Scheduler Admin (or equivalent custom permissions).
It also needs budget creation permissions on the specified billing account.
These are operator privileges, not roles assigned to the runtime identities.
The provider explicitly sends this project as the API quota project; the
Billing Budgets API requires that setting with user ADC credentials.

From the repository root:

```sh
# Copy only for a new checkout; this workspace already has the supplied IDs.
test -e infra/terraform.tfvars || cp infra/terraform.tfvars.example infra/terraform.tfvars
# Edit project_id, billing_account_id, and spark_submitter_members.
# Set trial_confirmed=true only after verifying credits and expiry.
gcloud config set project YOUR_PROJECT_ID
gcloud auth application-default set-quota-project YOUR_PROJECT_ID
gcloud billing projects describe YOUR_PROJECT_ID

terraform -chdir=infra init
make infra-check
terraform -chdir=infra plan -out=s02.tfplan
# Inspect the proposed resources, region, identities, and budget first.
terraform -chdir=infra apply s02.tfplan
terraform -chdir=infra output
make infra-smoke
terraform -chdir=infra plan -detailed-exitcode
```

After apply, the last command should exit 0 and show no changes. A first plan
will necessarily show resources to create. If enabling Service Usage requires
an initial bootstrap, enable `serviceusage.googleapis.com` and
`cloudresourcemanager.googleapis.com` in the existing project first. If there
are existing resources with these names, import them after checking ownership;
do not blindly delete or recreate them. The provider lock file is committed for
repeatable provider selection. Local tfvars, plans, state, and credentials are
excluded from Git.

`make infra-smoke` describes storage, SQL, secrets, and registry; runs all five
placeholder jobs; lists scheduler states; then runs `smoke_spark.py` on
Dataproc Serverless runtime 2.2. It uses two fixed executors and a ten-minute
TTL. The Spark job writes 1,000 rows to `checks/s02/<batch>/`, reads them back,
and requires a count of 1,000 and ID sum of 499,500. Reusing that output path
overwrites the smoke partition. Each invocation uses a distinct batch ID for
audit. [Runtime support](https://docs.cloud.google.com/managed-spark/docs/concepts/versions/serverless-versions)

The acceptance record below captures the real plan outcome, Cloud SQL
`RUNNABLE` state, successful job executions, all three paused schedules, and
Spark `SUCCEEDED` state and driver output. Describing a secret container
does not test a payload, and a placeholder job does not test the future
collector's source API or database queries. Those belong to S04-S09. If trial
quota prevents the Spark test, record the exact quota error and assess the
small-cluster option with the team; do not upgrade billing to bypass it.

Export the `data_bucket` and `cloudsql_connection_name` outputs as
`AIR_QUALITY_GCS_BUCKET` and `AIR_QUALITY_CLOUDSQL_CONNECTION_NAME` for later
jobs. SQL migrations and actual collector images remain in their owning
segments. Before enabling a schedule, replace its job image/command, configure
the required secret versions, and choose the S04-approved cadence. Commit the
corresponding `paused` change so a later apply preserves the intended state.

## Monthly estimate and cost controls

Planning estimate in USD for `us-central1`, checked November 8, 2026; 730
instance-hours/month unless stated. Amounts are gross usage before trial
credits and free allowances. S03 supplies the approximate raw ZIP volume;
other stored volumes below remain assumptions. All versions and soft-deleted
data count toward stored volume.

| Resource | Assumed usage and unit rate | Estimated monthly cost |
| --- | --- | ---: |
| SQL compute | 730 h × $0.0105/h | $7.67 |
| SQL SSD | 10 GiB × $0.17/GiB-month | $1.70 |
| SQL backups | 10 GiB total retained × $0.08/GiB-month | $0.80 |
| Data bucket: EPA raw ZIPs | S06 measured: 6.2554 GiB × $0.020/GiB-month, one generation | $0.13 |
| Data bucket: staging, curated, and older versions | Measure expansion and retained volume in S10–S12 | Not yet estimated |
| Spark scratch bucket | 5 GiB average × $0.020/GiB-month | $0.10 |
| Artifact Registry | 2 GiB × $0.10/GiB-month | $0.20 |
| Secret Manager | 3 active versions × $0.06/month | $0.18 |
| Scheduler | 3 jobs × $0.10/month; paused jobs count | $0.30 |
| Run placeholders | No executions while paused | $0.00 |
| Spark | No persistent cluster / no running batch | $0.00 |
| Service accounts, IAM, API enablement, VPC/subnet/firewall, SQL users/database, budget | No standalone recurring resource fee | $0.00 |
| **Example subtotal** | Excludes unmeasured data, executions, operations, logging overages, and egress | **$11.08** |

Rates: [Cloud SQL](https://cloud.google.com/sql/pricing),
[Cloud Storage](https://cloud.google.com/storage/pricing),
[Artifact Registry](https://cloud.google.com/artifact-registry/pricing),
[Secret Manager](https://cloud.google.com/secret-manager/pricing), and
[Cloud Scheduler](https://cloud.google.com/scheduler/pricing).
Initial empty buckets/repository/secrets cost less than this scenario; SQL
compute and provisioned disk alone are about $9.37/month. Backups grow with
retained data. At 1 TiB of total stored GCS versions, storage alone is about
$20.48/month. S03 found 6,716,687,360 approximate compressed bytes across
233 nonempty hourly ZIPs (1990–2026). S06 subsequently measured
6,716,684,039 stored bytes; the raw ZIP row above now uses that measurement.
This does not measure staged or curated volume; see
[`epa_manifest.md`](epa_manifest.md) for the snapshot and limitations.

Cloud Run jobs have a one-minute minimum charge per task. At 1 vCPU and 0.5
GiB, use $0.000018/vCPU-second plus $0.000002/GiB-second. Over 30 days, a
two-minute PurpleAir schedule, hourly AirNow, and five-minute sync generate
30,960 executions. Even if each finishes within a minute, this is about
**$35.29/month gross** for those jobs, bringing the example to roughly $46.37
before Spark, other jobs, and data movement. Longer tasks cost more. Free
allowances depend on usage elsewhere in the billing account. PurpleAir points
and Snowflake credits are separate budgets.
[Cloud Run pricing](https://cloud.google.com/run/pricing)

Standard serverless Spark is $0.06/DCU-hour. A default 12-DCU workload costs
about $0.72/hour compute; a ten-minute smoke run is about $0.12 compute plus
shuffle storage and GCS operations. Use actual DCU and shuffle usage from the
batch output for S12; this estimate is not evidence that the national backfill
fits the credit balance.
[Spark pricing](https://cloud.google.com/products/managed-service-for-apache-spark/pricing)

The $50 budget alerts on gross spend even when credits offset the bill. It
does not stop services or cap costs. Avoid keeping unused versions/images and
large temporary datasets. SQL's fixed 10 GiB disk needs a capacity check before
live ingestion expands. Pausing SQL stops compute charges but leaves storage
and possible idle-IP charges; destroy unused resources after preserving data
and evidence. To retire SQL through Terraform, first change
`deletion_protection=false` and apply; nonempty buckets require an explicit
data retention/deletion decision before removal. Do not run destroy on a shared
team environment without coordinating it.

## Acceptance record

| Check | Current result |
| --- | --- |
| Terraform format and provider schema validation | Passed locally |
| Four tests with a mocked Google provider | Passed: defaults, trial required, positive budget, matching billing account |
| Python lint/tests and shell syntax | Passed locally |
| Trial credits and expiry | Owner confirmed available credits; expires January 6, 2027 |
| Real Terraform plan/apply and no-change rerun | Deployed 78 managed resources; final plan exited 0 with no changes |
| Cloud SQL | `RUNNABLE`; `db-f1-micro`; connection `final-team-514205:us-central1:aq-operational` |
| Cloud Run | All five placeholder executions completed successfully |
| Scheduler | All three schedules verified `PAUSED` |
| Secrets and registry | Three containers replicated in `us-central1`; Docker repository verified |
| Budget | $50 USD/month; project-filtered gross-spend alerts at 50%, 90%, 100% |
| Small cloud Spark Parquet round trip | Batch `aq-s02-20261109052051` succeeded; 1,000 rows, ID sum 499,500 |

The first apply exposed the project's restriction on global secret locations
and the Billing Budgets API's quota-project requirement. The configuration now
uses explicit regional secret replicas, enables the Cloud Billing API, and
sends the project quota header. The billing-link precondition uses the billing
account's project list rather than the nullable billing field on the project
data source. The final plan confirms these fixes are reflected in cloud state.

The Spark service reported 711,000 milli-DCU-seconds and 72,000 shuffle-GB-seconds
for the verification batch. These are approximate service usage metrics, not
an invoice or backfill scale result. The durable evidence JSON records the
individual Run execution IDs and completion times; scratch driver logs remain
subject to their seven-day retention.
