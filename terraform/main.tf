data "aws_availability_zones" "available" {
  state = "available"
}

data "aws_ssm_parameter" "ubuntu_2404_ami" {
  name = "/aws/service/canonical/ubuntu/server/24.04/stable/current/amd64/hvm/ebs-gp3/ami-id"
}

locals {
  selected_availability_zone = coalesce(var.availability_zone, data.aws_availability_zones.available.names[0])
  effective_ami_id           = coalesce(var.ami_id, nonsensitive(data.aws_ssm_parameter.ubuntu_2404_ami.value))
  effective_ssh_key_name     = var.ssh_public_key != null ? coalesce(var.ssh_key_name, "${var.name_prefix}-key") : var.ssh_key_name
}
