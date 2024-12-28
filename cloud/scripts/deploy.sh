#!/bin/bash
set -e

# Check if a project ID is provided
if [ -z "$1" ]; then
  echo "Usage: $0 <project_id>"
  exit 1
fi

# Variables
PROJECT_ID="$1"

# Change to the terraform directory
cd cloud/terraform

# Initialize Terraform
terraform init

# Apply Terraform configuration with the provided project ID
terraform apply -var="gcp_project_id=${PROJECT_ID}" -auto-approve