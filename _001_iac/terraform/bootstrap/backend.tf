terraform {
  backend "s3" {
    bucket  = "data-pipeline-e2e-terraform-state"
    key     = "terraform/state/s3-datalake.tfstate"
    region  = "ap-southeast-1"
    encrypt = true
  }
}
