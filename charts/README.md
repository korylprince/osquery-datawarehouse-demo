# Helm Charts

Helm charts for the osquery data warehouse demo. The stack ingests osquery events into an Iceberg data lake and provides multiple query interfaces on top.

## Standalone Charts

These charts deploy individual components. They are referenced by the composite charts below.

### `osquery-datawarehouse-prereqs`

Creates the namespaces and auto-generates the secrets shared across the stack: garage admin token, Polaris token signing key, Polaris root credentials, DWH Chat auth & database credentials, and Superset secrets. Uses the [kubernetes-secret-generator](https://github.com/mittwald/kubernetes-secret-generator) controller.

### `build-images`

Builds container images in-cluster using buildah. For each image it creates a Job that checks whether the image already exists in the registry, fetches source code via git, and builds + pushes the image. By default it builds four images: `data-warehouse-mcp`, `dwh-chat`, `flink-osquery-job`, and `superset`.

### `garage`

Overlay resources for [Garage](https://garagehq.deuxfleurs.fr/), the S3-compatible object store. The Garage deployment itself is pulled from the upstream Garage Helm chart; this chart adds an init Job (`garage-init`) that creates the storage zone and warehouse bucket, plus the RBAC (ServiceAccount, Role, RoleBinding) the init Job needs to write secrets back to the cluster.

### `kafka`

Deploys a [Strimzi](https://strimzi.io/) [Kafka](https://kafka.apache.org/) cluster with a single dual-role (controller + broker) node pool. Creates the `osquery` KafkaTopic where osquery events are published. Also generates TLS certificates and a Traefik IngressRouteTCP for optional external (mTLS) access.

### `polaris`

Deploys [Apache Polaris](https://polaris.apache.org/), the Iceberg REST catalog. Polaris stores its metadata in a CloudNativePG-backed PostgreSQL database (`polaris-db`) and uses Garage as the Iceberg warehouse backend. An init Job (`polaris-init`) bootstraps the catalog by creating the default namespace, warehouse, and table privileges via a Python script.

### `trino`

Deploys the [Trino](https://trino.io/) query engine with an Iceberg catalog connected to Polaris for metadata and Garage for data files. Trino is the central query layer - Superset, the MCP server, and the Iceberg maintenance CronJobs all query through it.

### `flink`

Deploys the [Flink Kubernetes Operator](https://flink.apache.org/) and a `FlinkDeployment` that runs the osquery ingestion job. The job consumes osquery JSON events from Kafka, parses them, and writes them as Iceberg tables via Garage and Polaris. Two CronJobs run scheduled Iceberg maintenance against Trino: `iceberg-optimize` (hourly compaction) and `iceberg-cleanup` (daily expiration of old snapshots).

### `superset`

Deploys [Apache Superset](https://superset.apache.org/), the BI dashboard. Uses a custom Superset image (built by `build-images` to include database drivers) pre-configured with a Trino database connection pointing at the Iceberg catalog. On initial deployment an import Job loads a demo dataset (`osquery_os_version_per_day`), two charts ("macOS Version Over Time" timeseries bar and "Current macOS Distribution" pie chart), and a "macOS Versions" dashboard to showcase the stack out of the box.

### `data-warehouse-mcp`

Deploys the Data Warehouse MCP (Model Context Protocol) server. Provides AI agents with tool access to Trino/Iceberg: SQL query execution, schema introspection, file management in session sandboxes (chroot), and bash execution within isolated environments. Exported images are persisted on a PVC.

### `dwh-chat`

Deploys DWH Chat, an AI chat interface for the data warehouse. The source code lives in this repo (`images/dwh-chat/`). Built on Chainlit, it connects to the MCP server to let users query the data warehouse conversationally. The pod runs two containers: a Caddy reverse proxy serving the Chainlit frontend, and the Chainlit backend. Uses a CloudNativePG-backed PostgreSQL database for chat history.

## Composite Charts

These charts use the [Argo CD App of Apps](https://argo-cd.readthedocs.io/en/stable/operator-manual/cluster-bootstrapping/) pattern to deploy multiple child charts as Argo CD Applications, ordered via `sync-wave` annotations.

### `osquery-datawarehouse`

App-of-Apps root chart that deploys the full application stack. It creates Argo CD Applications for each component in three waves:

| Wave | Components |
|------|-----------|
| 1 | Strimzi Operator, Garage, Kafka |
| 2 | Trino, Polaris |
| 3 | Flink, Superset, DWH Chat, Data Warehouse MCP |

This chart also installs the upstream Garage chart (from `deuxfleurs-org/garage`) alongside the local `garage` overlay.

### `cluster-resources`

Top-level App-of-Apps root chart that provisions shared cluster infrastructure. It references `osquery-datawarehouse` and `osquery-datawarehouse-prereqs` as child Applications, and also deploys:

| Component | Purpose |
|-----------|---------|
| **[Argo CD](https://argo-cd.readthedocs.io)** | GitOps controller; bootstrapped first with custom health checks for sync-wave ordering |
| **[Traefik](https://traefik.io)** | Ingress controller; optionally creates a BasicAuth Middleware |
| **[cert-manager](https://cert-manager.io)** | TLS certificate management; creates ClusterIssuers (Let's Encrypt DNS-01 via Cloudflare), wildcard Certificates, and a Kafka CA for mTLS |
| **[CloudNative PG](https://cloudnative-pg.io)** | PostgreSQL operator used by Polaris, DWH Chat, and Grafana |
| **Observability** | [Prometheus](https://prometheus.io/) + [Grafana](https://grafana.com/) stack (Grafana backed by CloudNative PG) |
| **[Local Path Provisioner](https://github.com/rancher/local-path-provisioner)** | Default StorageClass for single-node clusters (e.g. k3s) |
| **[Registry](https://hub.docker.com/_/registry)** | In-cluster Docker registry for `build-images` |
| **[Replicator](https://github.com/mittwald/kubernetes-replicator)** | Cross-namespace secret replication (e.g. Polaris credentials to Trino and Flink) |
| **[Password Generator](https://github.com/mittwald/kubernetes-secret-generator)** | kubernetes-secret-generator controller for auto-generated secrets |

Deployment order is controlled by sync waves from `-15` (Argo CD, cert-manager, registry) through `0` (observability, osquery-datawarehouse).
