locals {
  dwh_chat_endpoints_with_keys = [for e in var.dwh_chat.endpoints : e if e.apiKeyRef != null]

  dwh_chat_secrets_yaml = join("\n---\n",
    [
      for e in local.dwh_chat_endpoints_with_keys :
      <<-EOF
apiVersion: v1
kind: Secret
metadata:
  name: dwh-chat-${e.apiKeyRef}
  namespace: dwh-chat
type: Opaque
stringData:
  api-key: ${jsonencode(e.apiKey)}
      EOF
    ]
  )

  dwh_chat_helm_values_yaml = join("\n",
    flatten([
      ["            endpoints:"],
      [for e in var.dwh_chat.endpoints : concat([
        "              - name: ${jsonencode(e.name)}",
        "                baseUrl: ${jsonencode(e.baseUrl)}",
        "                models: ${jsonencode(e.models)}",
      ], e.apiKeyRef != null ? ["                apiKeyFile: /secrets/${e.apiKeyRef}"] : [])],
      ["            apiKeySecrets:"],
      [for e in local.dwh_chat_endpoints_with_keys : [
        "              - secretName: dwh-chat-${e.apiKeyRef}",
        "                key: api-key",
        "                mountPath: /secrets/${e.apiKeyRef}"
      ]]
    ])
  )
}

resource "aws_key_pair" "this" {
  count = var.ssh_public_key != null ? 1 : 0

  key_name   = local.effective_ssh_key_name
  public_key = var.ssh_public_key
}

resource "aws_instance" "this" {
  ami                         = local.effective_ami_id
  instance_type               = var.instance_type
  subnet_id                   = aws_subnet.public.id
  vpc_security_group_ids      = [aws_security_group.instance.id]
  associate_public_ip_address = true
  key_name                    = local.effective_ssh_key_name
  user_data_replace_on_change = true
  user_data = templatefile("${path.module}/user_data.sh", {
    cluster_hostname_flag = var.cloudflare_zone != null ? "--tls-san ${var.cloudflare_zone}" : ""
    helm_cluster_manifest = templatefile("${path.module}/cluster_init.yaml", {
      argocd_repo_url           = var.argocd_repo_url
      argocd_repo_username      = var.argocd_repo_username
      argocd_repo_password      = var.argocd_repo_password
      domain_prefix             = coalesce(var.cloudflare_zone, "osquery.stream")
      cloudflare_enabled        = local.cloudflare_enabled
      cloudflare_zone           = var.cloudflare_zone
      cloudflare_wildcard_zone  = var.cloudflare_zone != null ? "*.${var.cloudflare_zone}" : null
      cloudflare_api_token      = var.cloudflare_api_token
      admin_password            = var.admin_password
      dwh_chat_secrets_yaml     = local.dwh_chat_secrets_yaml
      dwh_chat_helm_values_yaml = local.dwh_chat_helm_values_yaml
    })
  })

  root_block_device {
    volume_size           = var.root_volume_size_gb
    volume_type           = "gp3"
    encrypted             = true
    delete_on_termination = true
  }

  tags = { Name = var.name_prefix }
}
