# ===================================================================
# Temporary EC2 instance for one-time bulk ingestion.
#
# Gated by var.enable_bulk_instance — set to true to create,
# then set back to false (or terraform destroy -target) after use.
#
# Creates its own minimal VPC + public subnet so the project
# remains standalone with no external networking dependencies.
# ===================================================================


# -------------------------------------------------------------------
# Data: latest Amazon Linux 2023 AMI
# -------------------------------------------------------------------

data "aws_ami" "al2023" {
  count       = var.enable_bulk_instance ? 1 : 0
  most_recent = true
  owners      = ["amazon"]

  filter {
    name   = "name"
    values = ["al2023-ami-2023.*-x86_64"]
  }

  filter {
    name   = "virtualization-type"
    values = ["hvm"]
  }

  filter {
    name   = "architecture"
    values = ["x86_64"]
  }
}


# -------------------------------------------------------------------
# VPC + networking (minimal, just for bulk loader)
# -------------------------------------------------------------------

resource "aws_vpc" "bulk" {
  count      = var.enable_bulk_instance ? 1 : 0
  cidr_block = "10.99.0.0/24"

  enable_dns_support   = true
  enable_dns_hostnames = true

  tags = {
    Name    = "sp-ingest-bulk-vpc"
    Purpose = "one-time-bulk-ingestion"
  }
}

resource "aws_internet_gateway" "bulk" {
  count  = var.enable_bulk_instance ? 1 : 0
  vpc_id = aws_vpc.bulk[0].id

  tags = {
    Name = "sp-ingest-bulk-igw"
  }
}

resource "aws_subnet" "bulk_public" {
  count                   = var.enable_bulk_instance ? 1 : 0
  vpc_id                  = aws_vpc.bulk[0].id
  cidr_block              = "10.99.0.0/25"
  availability_zone       = "${var.aws_region}a"
  map_public_ip_on_launch = true

  tags = {
    Name = "sp-ingest-bulk-public"
  }
}

resource "aws_route_table" "bulk_public" {
  count  = var.enable_bulk_instance ? 1 : 0
  vpc_id = aws_vpc.bulk[0].id

  route {
    cidr_block = "0.0.0.0/0"
    gateway_id = aws_internet_gateway.bulk[0].id
  }

  tags = {
    Name = "sp-ingest-bulk-public-rt"
  }
}

resource "aws_route_table_association" "bulk_public" {
  count          = var.enable_bulk_instance ? 1 : 0
  subnet_id      = aws_subnet.bulk_public[0].id
  route_table_id = aws_route_table.bulk_public[0].id
}


# -------------------------------------------------------------------
# Security group: outbound HTTPS only, SSH from admin IP
# -------------------------------------------------------------------

resource "aws_security_group" "bulk_ec2" {
  count       = var.enable_bulk_instance ? 1 : 0
  name        = "sp-ingest-bulk-ec2-sg"
  description = "Bulk loader: outbound HTTPS, inbound SSH from admin"
  vpc_id      = aws_vpc.bulk[0].id

  # SSH from admin IP (if provided)
  dynamic "ingress" {
    for_each = var.bulk_admin_cidr != "" ? [1] : []
    content {
      description = "SSH from admin IP"
      from_port   = 22
      to_port     = 22
      protocol    = "tcp"
      cidr_blocks = [var.bulk_admin_cidr]
    }
  }

  # HTTPS outbound (Graph API, S3 gateway, Secrets Manager)
  egress {
    description = "HTTPS outbound"
    from_port   = 443
    to_port     = 443
    protocol    = "tcp"
    cidr_blocks = ["0.0.0.0/0"]
  }

  # HTTP outbound (yum repos for package install)
  egress {
    description = "HTTP outbound for package repos"
    from_port   = 80
    to_port     = 80
    protocol    = "tcp"
    cidr_blocks = ["0.0.0.0/0"]
  }

  # DNS outbound
  egress {
    description = "DNS UDP"
    from_port   = 53
    to_port     = 53
    protocol    = "udp"
    cidr_blocks = ["0.0.0.0/0"]
  }

  egress {
    description = "DNS TCP"
    from_port   = 53
    to_port     = 53
    protocol    = "tcp"
    cidr_blocks = ["0.0.0.0/0"]
  }

  tags = {
    Name    = "sp-ingest-bulk-ec2-sg"
    Purpose = "one-time-bulk-ingestion"
  }
}


# -------------------------------------------------------------------
# EC2 instance
# -------------------------------------------------------------------

resource "aws_instance" "bulk_loader" {
  count = var.enable_bulk_instance ? 1 : 0

  ami                    = data.aws_ami.al2023[0].id
  instance_type          = "t3.xlarge"
  iam_instance_profile   = aws_iam_instance_profile.bulk_ec2.name
  subnet_id              = aws_subnet.bulk_public[0].id
  vpc_security_group_ids = [aws_security_group.bulk_ec2[0].id]
  key_name               = var.bulk_key_pair_name != "" ? var.bulk_key_pair_name : null

  root_block_device {
    volume_size           = 100
    volume_type           = "gp3"
    encrypted             = true
    delete_on_termination = true
  }

  user_data = base64encode(templatefile("${path.module}/templates/bulk-userdata.sh.tftpl", {
    aws_region          = var.aws_region
    s3_bucket           = var.s3_bucket_name
    s3_source_prefix    = "source"
    s3_extracted_prefix = "extracted"
    delta_table         = var.delta_table_name
    registry_table      = var.registry_table_name
    secret_prefix       = "sp-ingest/"
    sharepoint_site     = var.sharepoint_site_name
    excluded_folders    = var.excluded_folders
    sns_topic_arn       = ""
  }))

  tags = {
    Name    = "sp-ingest-bulk-loader"
    Purpose = "one-time-bulk-ingestion"
  }
}
