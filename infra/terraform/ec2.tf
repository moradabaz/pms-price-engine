# ---------------------------------------------------------------------------
# On-demand portfolio demo host (EC2 + Elastic IP + Caddy reverse proxy)
#
# This instance runs the whole local docker-compose stack (Kafka, Flink,
# LocalStack, the Streamlit dashboard, and the Astro landing page) standing
# in for a real cloud deployment. It is meant to be started before an
# interview/demo and stopped right after — see infra/aws/start-demo.sh and
# stop-demo.sh. It is NOT started or stopped by Terraform itself.
# ---------------------------------------------------------------------------

locals {
  demo_instance_type     = "m5.xlarge" # 16GB RAM: Kafka+ZK+Connect+Flink JM+TM+dashboard JVMs
  demo_root_volume_gb    = 50          # gp3, Docker images + Kafka/Postgres/Iceberg data
  demo_ssh_port          = 22
  demo_http_port         = 80
  demo_https_port        = 443
  demo_repo_url          = "https://github.com/moradabaz/pms-price-engine.git"
  demo_repo_clone_path   = "/opt/pms-price-engine"
  demo_app_user          = "ec2-user"
  caddy_install_version  = "2.8.4"
  buildx_install_version = "0.19.3" # compose build requires >= 0.17.0
}

# ---------------------------------------------------------------------------
# Security group: only SSH (from the operator's own IP) + HTTP/HTTPS for
# Caddy. Every other port the stack uses (5432, 9092, 8081, 8501, 4566)
# stays internal to the EC2 instance's Docker network, never exposed here.
# ---------------------------------------------------------------------------
resource "aws_security_group" "demo" {
  name        = "pms-price-engine-demo"
  description = "Portfolio demo host: SSH (restricted) + HTTP/HTTPS only"

  ingress {
    description = "SSH from operator IP only"
    from_port   = local.demo_ssh_port
    to_port     = local.demo_ssh_port
    protocol    = "tcp"
    cidr_blocks = [var.ssh_allowed_cidr]
  }

  ingress {
    description = "HTTP (Caddy ACME challenge + redirect to HTTPS)"
    from_port   = local.demo_http_port
    to_port     = local.demo_http_port
    protocol    = "tcp"
    cidr_blocks = ["0.0.0.0/0"]
  }

  ingress {
    description = "HTTPS (Caddy reverse proxy)"
    from_port   = local.demo_https_port
    to_port     = local.demo_https_port
    protocol    = "tcp"
    cidr_blocks = ["0.0.0.0/0"]
  }

  egress {
    description = "Unrestricted outbound (Docker pulls, dnf, Lets Encrypt)"
    from_port   = 0
    to_port     = 0
    protocol    = "-1"
    cidr_blocks = ["0.0.0.0/0"]
  }

  tags = local.common_tags
}

# ---------------------------------------------------------------------------
# AMI: latest Amazon Linux 2023, resolved at plan/apply time instead of
# pinned, so the instance always boots with current security patches.
# ---------------------------------------------------------------------------
data "aws_ami" "al2023" {
  most_recent = true
  owners      = ["amazon"]

  filter {
    name   = "name"
    values = ["al2023-ami-*-x86_64"]
  }

  filter {
    name   = "virtualization-type"
    values = ["hvm"]
  }
}

resource "aws_instance" "demo" {
  ami                    = data.aws_ami.al2023.id
  instance_type          = local.demo_instance_type
  key_name               = var.ssh_key_name
  vpc_security_group_ids = [aws_security_group.demo.id]

  root_block_device {
    volume_type = "gp3"
    volume_size = local.demo_root_volume_gb
  }

  # Installs Docker + Caddy and clones the repo. Deliberately does NOT run
  # `docker compose up` or start Caddy — that happens on demand via
  # infra/aws/start-demo.sh, so a freshly-created (or restarted) instance
  # never silently starts serving a half-configured demo.
  user_data = templatefile("${path.module}/templates/user_data.sh.tftpl", {
    repo_url        = local.demo_repo_url
    repo_clone_path = local.demo_repo_clone_path
    app_user        = local.demo_app_user
    caddy_version   = local.caddy_install_version
    buildx_version  = local.buildx_install_version
  })

  tags = merge(local.common_tags, {
    Name = "pms-price-engine-demo"
  })
}

# ---------------------------------------------------------------------------
# Elastic IP: stays the same across every stop/start cycle, so the DNS
# record (and the Let's Encrypt certificate tied to it) only has to be set
# up once, not re-done before every interview.
# ---------------------------------------------------------------------------
resource "aws_eip" "demo" {
  instance = aws_instance.demo.id
  domain   = "vpc"

  tags = local.common_tags
}
