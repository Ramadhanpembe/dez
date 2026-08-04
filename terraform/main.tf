terraform {
    required_providers {
        google = {
            source = "hashicorp/google"
            version = "~> 5.0"
        }
    }

    backend "gcs" {
        bucket = "dez-terraform-demo-504509-bucket"
        prefix = "terraform/state"
    }
}

provider "google" {
    project = var.project_id
    region  = var.region
}

module "storage_and_bq" {
    source          = "./modules/gcs_bigquery"
    project_id      = var.project_id
    region          = var.region
    bucket_name     = var.bucket_name
    bq_dataset_name = var.bq_dataset_name  
}

# moved {
#     from    = google_storage_bucket.demo_bucket
#     to      = module.storage_and_bq.google_storage_bucket.demo_bucket        
# }

# moved {
#     from    = google_bigquery_dataset.demo_dataset
#     to      = module.storage_and_bq.google_bigquery_dataset.demo_dataset
# }

