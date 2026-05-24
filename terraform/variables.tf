variable "aws_region" {
  description = "AWS region to deploy into."
  type        = string
  default     = "us-east-1"
}

variable "aws_access_key_id" {
  description = "Optional AWS access key ID. Leave null to use the normal AWS credential chain."
  type        = string
  default     = null
  nullable    = true
  sensitive   = true
}

variable "aws_secret_access_key" {
  description = "Optional AWS secret access key. Leave null to use the normal AWS credential chain."
  type        = string
  default     = null
  nullable    = true
  sensitive   = true
}

variable "aws_session_token" {
  description = "Optional AWS session token for temporary credentials."
  type        = string
  default     = null
  nullable    = true
  sensitive   = true
}

variable "aws_profile" {
  description = "Optional AWS shared config profile."
  type        = string
  default     = null
  nullable    = true
}

variable "name_prefix" {
  description = "Prefix used for naming AWS resources."
  type        = string
  default     = "osquery-demo"
}

variable "availability_zone" {
  description = "Optional availability zone override. Leave null to use the first available AZ in the selected region."
  type        = string
  default     = null
  nullable    = true
}

variable "vpc_cidr" {
  description = "CIDR block for the custom VPC."
  type        = string
  default     = "10.0.0.0/16"
}

variable "public_subnet_cidr" {
  description = "CIDR block for the public subnet."
  type        = string
  default     = "10.0.1.0/24"
}

variable "instance_type" {
  description = "EC2 instance type."
  type        = string
  default     = "m8a.xlarge"
}

variable "ami_id" {
  description = "Optional AMI override. Leave null to use the latest Ubuntu 24.04 LTS amd64 AMI."
  type        = string
  default     = null
  nullable    = true
}

variable "ssh_key_name" {
  description = "Existing AWS key pair name to use, or the name to create when ssh_public_key is provided."
  type        = string
  default     = null
  nullable    = true
}

variable "ssh_public_key" {
  description = "Optional public key material. If set, this repo creates the AWS key pair."
  type        = string
  default     = null
  nullable    = true
}

variable "ssh_allowed_cidrs" {
  description = "CIDR blocks allowed to reach SSH on port 22."
  type        = list(string)
  default     = ["0.0.0.0/0"]
}

variable "kubectl_allowed_cidrs" {
  description = "CIDR blocks allowed to reach the Kubernetes API server on port 6443."
  type        = list(string)
  default     = ["0.0.0.0/0"]
}

variable "https_allowed_cidrs" {
  description = "CIDR blocks allowed to reach the Kubernetes API server on port 6443."
  type        = list(string)
  default     = ["0.0.0.0/0"]
}

variable "root_volume_size_gb" {
  description = "Root EBS volume size in GB."
  type        = number
  default     = 50
}

variable "argocd_repo_url" {
  description = "Optional Git repository URL for the private osquery-datawarehouse-demo Argo CD repository Secret. Set together with argocd_repo_username and argocd_repo_password."
  type        = string
  default     = "https://github.com/korylprince/osquery-datawarehouse-demo.git"
  nullable    = true
}

variable "argocd_repo_username" {
  description = "Optional Git username for the private osquery-datawarehouse-demo Argo CD repository Secret. Set together with argocd_repo_url and argocd_repo_password."
  type        = string
  default     = null
  nullable    = true
}

variable "argocd_repo_password" {
  description = "Optional Git password or personal access token for the private osquery-datawarehouse-demo Argo CD repository Secret. Set together with argocd_repo_url and argocd_repo_username."
  type        = string
  default     = null
  nullable    = true
  sensitive   = true
}

variable "cloudflare_zone" {
  description = "Cloudflare zone name, such as example.com. Set together with cloudflare_api_token to create DNS records."
  type        = string
  default     = null
  nullable    = true
}

variable "cloudflare_api_token" {
  description = "Cloudflare API token used to manage DNS records. Set together with cloudflare_zone."
  type        = string
  default     = null
  nullable    = true
  sensitive   = true
}

variable "admin_password" {
  description = "Optional admin password for Argo CD and Flink external hosts. When set, creates a basic-auth secret and Traefik Middleware to protect argo.<domain> and flink.<domain>."
  type        = string
  default     = null
  nullable    = true
  sensitive   = true
}

variable "dwh_chat" {
  description = "dwh-chat AI endpoint configuration. Each endpoint with an apiKeyRef/apiKey pair creates a K8s Secret mounted into the dwh-chat pod."
  type = object({
    endpoints = list(object({
      name      = string
      baseUrl   = string
      models    = list(string)
      apiKeyRef = optional(string, null)
      apiKey    = optional(string, null)
    }))
  })
  default = {
    endpoints = [
      {
        name    = "Local"
        baseUrl = "http://model-router.model-api-operator.svc.cluster.local:8080"
        models  = ["my-model"]
      }
    ]
  }
  sensitive = true
}
