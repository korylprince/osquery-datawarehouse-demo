# Terraform - AWS Infrastructure

Provisions a single EC2 host running **k3s + Argo CD** for the osquery data warehouse demo.

## Quick Start

```bash
tofu init
cp terraform.tfvars.example terraform.tfvars
# fill in variables in terraform.tfvars
tofu apply
```

## What It Creates

- VPC, public subnet, internet gateway, route table
- Security groups (SSH, k8s API, HTTPS)
- EC2 instance (Ubuntu 24.04, `m8a.xlarge` by default)
- Cloudflare DNS records
- SSH key pair

User-data bootstraps k3s, installs Argo CD, and applies the `charts/cluster-resources` Helm chart to deploy the full data warehouse stack.

## Useful Outputs

```bash
tofu output -raw public_ip
tofu output -raw cloudflare_root_record
```

## Destroy

```bash
tofu destroy
```

## Variables

See `variables.tf` for the full list. Key ones:

| Variable | Description |
|---|---|
| `ssh_public_key` | SSH public key for the EC2 instance |
| `cloudflare_zone` | Cloudflare zone (e.g. `osquery.stream`) |
| `cloudflare_api_token` | Cloudflare API token for DNS management |
| `argocd_repo_username` | Git username for the Argo CD repo |
| `argocd_repo_password` | Git token/password for the Argo CD repo |
| `admin_password` | Basic-auth password for Argo CD and Flink |
| `dwh_chat.endpoints` | AI endpoint config (name, baseUrl, models, apiKey) |
