locals {
  base_apis = ["container.googleapis.com"]
}

module "enable_google_apis" {
  source  = "terraform-google-modules/project-factory/google//modules/project_services"
  version = "~> 17.0"

  project_id                  = var.gcp_project_id
  disable_services_on_destroy = false

  activate_apis = local.base_apis
}

resource "google_container_cluster" "my_cluster" {
  name     = var.name
  location = var.zone

  initial_node_count = 2
  node_config {
    machine_type = "e2-standard-2"
  }

  deletion_protection = false

  depends_on = [
    module.enable_google_apis
  ]
}