output "instance_id" {
  description = "EC2 instance ID."
  value       = aws_instance.this.id
}

output "public_ip" {
  description = "Direct public IPv4 address assigned to the EC2 instance."
  value       = aws_instance.this.public_ip
}

output "public_dns" {
  description = "Public DNS name assigned to the EC2 instance."
  value       = aws_instance.this.public_dns
}

output "vpc_id" {
  description = "Custom VPC ID."
  value       = aws_vpc.this.id
}

output "subnet_id" {
  description = "Public subnet ID."
  value       = aws_subnet.public.id
}

output "availability_zone" {
  description = "Availability zone used by the subnet and EC2 instance."
  value       = local.selected_availability_zone
}

output "ami_id" {
  description = "AMI ID used by the EC2 instance."
  value       = local.effective_ami_id
}

output "ssh_key_name" {
  description = "SSH key pair name in AWS, if one is configured."
  value       = local.effective_ssh_key_name
}

output "cloudflare_root_record" {
  description = "Cloudflare zone apex record created for the instance, if Cloudflare is configured."
  value       = local.cloudflare_enabled ? var.cloudflare_zone : null
}

output "cloudflare_service_records" {
  description = "Cloudflare service hostnames created for the instance, if Cloudflare is configured."
  value       = local.cloudflare_enabled ? [for label in sort(tolist(local.cloudflare_service_labels)) : "${label}.${var.cloudflare_zone}"] : []
}

output "kafka_broker" {
  description = "External Kafka bootstrap broker address, if Cloudflare is configured."
  value       = local.cloudflare_enabled ? "kafka.${var.cloudflare_zone}:9093" : null
}
