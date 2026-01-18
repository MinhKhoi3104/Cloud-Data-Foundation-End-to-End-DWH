# Terraform Infrastructure Documentation

## 📋 Overview

This document describes the structure and usage of Terraform modules to deploy infrastructure for the End-to-End Data Pipeline project. The infrastructure is divided into 3 main modules:

1. **Bootstrap** - Creates S3 bucket to store Terraform state
2. **S3** - Creates S3 data lake buckets for data pipeline

![s3_bucket](/image/s3_buckets.png)

3. **Redshift** - Creates Redshift Serverless for data warehouse

![redshift_overview](/image/redshift_overview.png)

## 🏗️ Architecture

```
_001_iac/terraform/
├── bootstrap/          # First module to deploy - creates the S3 bucket used to store Terraform state (backend)
├── s3/                # Module creates S3 data lake buckets
└── redshift/          # Module creates Redshift Serverless infrastructure
```

## 📦 Module 1: Bootstrap

### Purpose
This module creates an S3 bucket to store Terraform state files. This is the **required first module** that must be deployed before deploying other modules.

### File Structure
- `main.tf` - Defines S3 bucket and related configurations
- `provider.tf` - AWS provider configuration
- `backend.tf` - S3 backend configuration 

### Resources Created
- **S3 Bucket**: `data-pipeline-e2e-terraform-state`
  - Versioning enabled
  - Public access blocked
  - Server-side encryption (AES256)

### Backend Configuration
```hcl
backend "s3" {
  bucket  = "data-pipeline-e2e-terraform-state"
  key     = "terraform/state/s3-datalake.tfstate"
  region  = "ap-southeast-1"
  encrypt = true
}
```

### How to Deploy

```bash
cd _001_iac/terraform/bootstrap

# Initialize Terraform
terraform init

# Plan changes
terraform plan

# Apply changes
terraform apply
```

### Notes
- Bucket name: `data-pipeline-e2e-terraform-state`
- Region: `ap-southeast-1`
- After deployment, other modules will use this bucket as backend

---

## 📦 Module 2: S3 Data Lake

### Purpose
This module creates S3 buckets for the data lake, including:
- Main data bucket with versioning and lifecycle policies
- Log bucket for S3 access logs

### File Structure
- `main.tf` - Defines S3 buckets and configurations
- `variables.tf` - Variable definitions

### Resources Created

#### 1. Main Data Bucket
- **Name**: `{bucket_name}-{random_suffix}`
- **Default name**: `data-pipeline-e2e-datalake-98c619f9`
- **Features**:
  - Versioning enabled
  - Public access blocked
  - Lifecycle rules:
    - Non-current versions → STANDARD_IA after 30 days
    - Non-current versions → GLACIER after 60 days
    - Non-current versions → Expire after 90 days
  - Access logging enabled

#### 2. Log Bucket
- **Name**: `s3-access-logs-98c619f9`
- **Purpose**: Store access logs from main bucket
- Public access blocked

### Variables

| Variable | Type | Default | Description |
|----------|------|---------|-------------|
| `bucket_name` | string | `"data-pipeline-e2e-datalake"` | Base name for S3 data lake bucket |


### How to Deploy

```bash
cd _001_iac/terraform/s3

# Initialize Terraform (will automatically configure S3 backend)
terraform init

# Plan changes
terraform plan

# Apply changes
terraform apply
```

### Outputs
This module does not have defined outputs, but you can query resources after deployment.

---

## 📦 Module 3: Redshift Serverless

### Purpose
This module creates Redshift Serverless infrastructure including:
- VPC and networking (subnets, route tables, internet gateway)
- Security groups
- IAM roles and policies
- Redshift Serverless namespace and workgroup

### File Structure
- `network.tf` - VPC, subnets, route tables, internet gateway
- `security-group.tf` - Security group for Redshift
- `redshift-iam.tf` - IAM roles and policies
- `redshift-serverless.tf` - Redshift Serverless namespace and workgroup
- `variables.tf` - Variable definitions
- `outputs.tf` - Module outputs
- `provider.tf` - AWS provider configuration
- `terraform.tfvars` - Variable values (⚠️ contains sensitive data)

### Resources Created

#### 1. Networking
- **VPC**: CIDR `10.20.0.0/16`
  - DNS hostnames enabled
- **Subnets**: 3 subnets across 3 availability zones
  - Subnet 1: `10.20.1.0/24` (AZ1)
  - Subnet 2: `10.20.2.0/24` (AZ2)
  - Subnet 3: `10.20.3.0/24` (AZ3)
- **Internet Gateway**: Enables public access
- **Route Table**: Routes traffic to Internet Gateway
- **Route Table Associations**: Associates route table with subnets

#### 2. Security Group
- **Port**: 5439 (Redshift default port)
- **Ingress**: TCP port 5439 from `0.0.0.0/0` (⚠️ should be restricted in production)

#### 3. IAM
- **IAM Role**: `{app_name}-{app_environment}-redshift-serverless-role`
- **Policies**:
  - S3 full access (inline policy)
  - AmazonRedshiftAllCommandsFullAccess (managed policy)

#### 4. Redshift Serverless
- **Namespace**: 
  - Name: `my-project-e2e-nsp` (configurable)
  - Database: `my-project-e2e-dtb` (configurable)
  - Admin username/password: from `terraform.tfvars`
- **Workgroup**:
  - Name: `my-project-e2e-wg` (configurable)
  - Base capacity: 32 RPUs (configurable: 32-512, increments of 8)
  - Publicly accessible: `true` (configurable)
  - Security groups: Redshift security group
  - Subnets: 3 subnets across 3 AZs

### Variables

| Variable | Type | Default | Description |
|----------|------|---------|-------------|
| `aws_region` | string | - | AWS region |
| `app_name` | string | - | Application name |
| `app_environment` | string | - | Environment (dev, test, staging, prod) |
| `redshift_serverless_vpc_cidr` | string | - | VPC CIDR block |
| `redshift_serverless_subnet_1_cidr` | string | - | Subnet 1 CIDR |
| `redshift_serverless_subnet_2_cidr` | string | - | Subnet 2 CIDR |
| `redshift_serverless_subnet_3_cidr` | string | - | Subnet 3 CIDR |
| `redshift_serverless_namespace_name` | string | - | Namespace name |
| `redshift_serverless_database_name` | string | - | Database name |
| `redshift_serverless_admin_username` | string | - | Admin username |
| `redshift_serverless_admin_password` | string | - | Admin password (sensitive) |
| `redshift_serverless_workgroup_name` | string | - | Workgroup name |
| `redshift_serverless_base_capacity` | number | `32` | Base capacity in RPUs (32-512, increments of 8) |
| `redshift_serverless_publicly_accessible` | bool | `false` | Public access flag |

### Current Configuration (terraform.tfvars)

```hcl
aws_region     = "ap-southeast-1"
app_name       = "project-e2e"
app_environment = "dev"

# Network
redshift_serverless_vpc_cidr      = "10.20.0.0/16"
redshift_serverless_subnet_1_cidr = "10.20.1.0/24"
redshift_serverless_subnet_2_cidr = "10.20.2.0/24"
redshift_serverless_subnet_3_cidr = "10.20.3.0/24"

# Redshift Serverless
redshift_serverless_namespace_name      = "my-project-e2e-nsp"
redshift_serverless_database_name       = "my-project-e2e-dtb"
redshift_serverless_admin_username      = "admin"
redshift_serverless_admin_password      = "Devdata123"
redshift_serverless_workgroup_name      = "my-project-e2e-wg"
redshift_serverless_base_capacity       = 32
redshift_serverless_publicly_accessible = true
```

### Outputs

| Output | Description |
|--------|-------------|
| `redshift_serverless_vpc_id` | VPC ID |
| `redshift_serverless_vpc_cidr` | VPC CIDR |
| `redshift_serverless_subnet_az1_id` | Subnet AZ1 ID |
| `redshift_serverless_subnet_az2_id` | Subnet AZ2 ID |
| `redshift_serverless_subnet_az3_id` | Subnet AZ3 ID |
| `redshift_serverless_subnet_az1_cidr` | Subnet AZ1 CIDR |
| `redshift_serverless_subnet_az2_cidr` | Subnet AZ2 CIDR |
| `redshift_serverless_subnet_az3_cidr` | Subnet AZ3 CIDR |
| `redshift_serverless_iam` | IAM role ARN |
| `redshift_serverless_namespace_id` | Namespace ID |
| `redshift_serverless_namespace_arn` | Namespace ARN |

### How to Deploy

```bash
cd _001_iac/terraform/redshift

# Initialize Terraform
terraform init

# Plan changes
terraform plan

# Apply changes (⚠️ may take 10-30 minutes)
terraform apply
```

### Dependencies
This module has the following dependencies:
- Namespace depends on IAM role policies
- Workgroup depends on:
  - Namespace
  - Route table associations
  - Internet gateway
  - Security group

### Important Notes

1. **Deployment Time**: Redshift Serverless may take 10-30 minutes to create/modify, especially when changing network settings. This is normal AWS behavior.

2. **Security**: 
   - Current security group allows access from `0.0.0.0/0`. Should be restricted in production.
   - File `terraform.tfvars` contains sensitive data (password).

3. **Network**: 
   - Module automatically creates Internet Gateway and Route Tables to support public access.
   - If `publicly_accessible = false`, NAT Gateway may be needed for private subnets.

4. **Cost**: 
   - Redshift Serverless charges by RPU-hours.
   - Base capacity of 32 RPUs is the minimum.
   - Can auto-scale based on workload.

---

## 🔄 Deployment Order

Modules must be deployed in the following order:

1. **Bootstrap** (required first)
   ```bash
   cd bootstrap && terraform init && terraform apply
   ```

2. **S3** (after bootstrap is complete)
   ```bash
   cd s3 && terraform init && terraform apply
   ```

3. **Redshift** (can be deployed independently or after S3)
   ```bash
   cd redshift && terraform init && terraform apply
   ```

---

## 🔒 Security Best Practices

### 1. State Files
- ✅ State files stored in S3 with encryption
- ✅ Versioning enabled for state bucket
- ✅ Public access blocked

### 2. Sensitive Data
- ⚠️ `terraform.tfvars` is not ignored in `.gitignore`
- ✅ Passwords marked as `sensitive = true`
- ⚠️ Should use AWS Secrets Manager or Parameter Store for production

### 3. Network Security
- ⚠️ Current security group is too open (`0.0.0.0/0`)
- ✅ Public access can be disabled with `publicly_accessible = false`
- ✅ VPC is created separately, not using default VPC

### 4. IAM
- ⚠️ IAM role has S3 full access - should be restricted in production
- ✅ Uses least privilege principle

---

## 🛠️ Troubleshooting

### Issue: Terraform hangs when modifying Redshift
**Cause**: Redshift Serverless modify operations may take 10-30 minutes.

**Solution**:
- Check AWS Console to view resource status
- Check CloudWatch Logs for errors
- Run `terraform plan` first to preview changes
- Ensure all dependencies are created (IGW, route tables, etc.)

### Issue: Module cannot find S3 backend
**Cause**: Bootstrap module has not been deployed.

**Solution**: Deploy bootstrap module first.

### Issue: Insufficient availability zones for subnets
**Cause**: Region does not have 3 AZs or data source error.

**Solution**: Check `data.aws_availability_zones.available` and ensure region has enough AZs.

---

## 📝 Maintenance

### Update Variables
Edit `terraform.tfvars` file in each module and run:
```bash
terraform plan
terraform apply
```

### Destroy Resources
⚠️ **Warning**: This command will delete all resources!

```bash
terraform destroy
```

### State Management
State files are stored in S3 bucket `data-pipeline-e2e-terraform-state`. You can:
- View state: `terraform show`
- List resources: `terraform state list`
- Import existing resources: `terraform import`

---

## 📚 References

- [Terraform AWS Provider Documentation](https://registry.terraform.io/providers/hashicorp/aws/latest/docs)
- [Terraform S3 Backend](https://www.terraform.io/docs/language/settings/backends/s3.html)
- [Terraform S3 code tutorial](https://github.com/rahulwagh/aws-terraform-course-repo/tree/main/s3)
- [Terraform Redshift code tutorial](https://github.com/KopiCloud/terraform-aws-redshift-serverless/tree/main)

---

## 📞 Support

If you encounter issues, check:
1. AWS credentials are configured correctly
2. IAM permissions are sufficient to create resources
3. Region has enough availability zones
4. Terraform version is compatible (>= 1.0)

---

**Last Updated**: 2025-12-27
**Terraform Version**: >= 1.0
**AWS Provider Version**: >= 5.0

