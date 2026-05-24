# AGENTS

## Infra up/down
- Infra lives in `terraform/` and boots a single EC2 host running `k3s` + Argo CD via `user_data.sh`.
- First-time setup: `cd terraform && cp terraform.tfvars.example terraform.tfvars`, then fill AWS/SSH values and any optional Cloudflare/Argo CD repo settings.
- Auth with `aws login` (or use the profile/env-var options shown in `terraform.tfvars.example`), then run `tofu init`.
- Bring infra up with `cd terraform && tofu apply`.
- Tear infra down with `cd terraform && tofu destroy`.
- Handy outputs: `tofu output -raw public_ip` and `tofu output -raw cloudflare_root_record`.

## Cluster inspection (`scripts/*`)
- `./scripts/run.sh <cmd>` SSHes to the cluster host as `ubuntu` and runs the command there.
- `./scripts/kubectl.sh ...` is a wrapper for `./scripts/run.sh sudo k3s kubectl ...`.
  - this runs over SSH, so local files with kubectl won't work
- `./scripts/run.sh -L 8080:localhost:8080` works for SSH port-forwarding too.
- `run.sh` currently resolves the host via `terraform output -raw cloudflare_root_record`; if Cloudflare is not configured, use `tofu output -raw public_ip` for manual SSH.

## Repo layout
- `terraform/`: AWS + optional Cloudflare infra, plus bootstrap files like `cluster_init.yaml` and `user_data.sh`.
- `charts/cluster-resources`: top-level Argo CD app-of-apps for shared cluster services and the data warehouse stack.
- `charts/osquery-datawarehouse-prereqs`: namespaces and generated shared secrets.
- `charts/osquery-datawarehouse`: Argo CD app-of-apps for workload charts.
- `charts/{garage,kafka,polaris,trino,flink,superset,data-warehouse-mcp,dwh-chat,build-images}`: workload charts.
- `images/`: source/build context for custom images (`data-warehouse-mcp`, `dwh-chat`, `flink-osquery-job`, `superset`).
- `osquery-extension/`: Go osquery extension exposing AWS Cost Explorer billing as a queryable `aws_billing` table.
- `scripts/`: SSH/kubectl helpers.
- `synthetic_data/`: sample data.

## How the charts fit together
- Bootstrap chain: Terraform/user-data installs `k3s` + Argo CD, then applies `charts/cluster-resources`, which in turn installs shared services plus `osquery-datawarehouse-prereqs` and `osquery-datawarehouse`.
- `osquery-datawarehouse-prereqs` creates the namespaces and generated secrets that later apps consume.
- `osquery-datawarehouse` sync order is roughly: `build-images` (`-10`), `garage` + `kafka` (`1`, strimzi-operator pulled from OCI), `polaris` + `trino` (`2`), then `flink` + `superset` + `data-warehouse-mcp` + `dwh-chat` (`3`).
- Data flow: Kafka ingests events, Flink writes Iceberg tables via Polaris using Garage S3, Trino queries the lake, Superset and the MCP server sit on top of Trino, and DWH Chat talks to the MCP server.

## Registry
- Images are pushed to a Docker registry exposed via NodePort (`registry-nodeport` in `registry-docker-registry` namespace, port 31234).
- To force a rebuild, delete the existing image from inside the cluster using `gcr.io/go-containerregistry/crane:debug`. Run via `scripts/kubectl.sh run -n kube-system`:
  1. Get the digest: `crane digest registry-docker-registry.registry.svc.cluster.local:5000/<image>:<tag> --insecure`
  2. Delete by digest: `crane delete 'registry-docker-registry.registry.svc.cluster.local:5000/<image>@<digest>' --insecure`
  - `crane delete` by tag alone can fail with `DIGEST_INVALID` on this registry; deleting by digest is more reliable.
