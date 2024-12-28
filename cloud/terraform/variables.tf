variable "gcp_project_id" {
  type        = string
  description = "GCP project ID"
}

variable "name" {
  type        = string
  description = "GKE cluster name"
  default     = "spark-cluster"
}

variable "region" {
  type        = string
  description = "GKE cluster region"
  default     = "europe-west6"
}

variable "zone" {
  type        = string
  description = "GKE cluster zone"
  default     = "europe-west6-b"
}