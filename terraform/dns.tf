locals {
  cloudflare_api_token_set  = nonsensitive(var.cloudflare_api_token) != null
  cloudflare_enabled        = var.cloudflare_zone != null && local.cloudflare_api_token_set
  cloudflare_service_labels = toset(["kafka", "superset", "chat", "argo", "flink", "grafana"])
}

check "cloudflare_record_configuration" {
  assert {
    condition     = (var.cloudflare_zone != null) == local.cloudflare_api_token_set
    error_message = "Set both cloudflare_zone and cloudflare_api_token, or leave both unset."
  }
}

data "cloudflare_zones" "this" {
  count = local.cloudflare_enabled ? 1 : 0

  name = var.cloudflare_zone
}

resource "cloudflare_dns_record" "root" {
  count = local.cloudflare_enabled ? 1 : 0

  zone_id = data.cloudflare_zones.this[0].result[0].id
  name    = "@"
  type    = "A"
  content = aws_instance.this.public_ip
  ttl     = 60
  proxied = false
}

resource "cloudflare_dns_record" "service" {
  for_each = local.cloudflare_enabled ? local.cloudflare_service_labels : toset([])

  zone_id = data.cloudflare_zones.this[0].result[0].id
  name    = each.value
  type    = "CNAME"
  content = var.cloudflare_zone
  ttl     = 60
  proxied = false
}
