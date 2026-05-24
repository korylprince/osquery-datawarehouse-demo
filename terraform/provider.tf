provider "aws" {
  region = var.aws_region

  access_key = var.aws_access_key_id
  secret_key = var.aws_secret_access_key
  token      = var.aws_session_token
  profile    = var.aws_profile
}

provider "cloudflare" {
  api_token = var.cloudflare_api_token
}
