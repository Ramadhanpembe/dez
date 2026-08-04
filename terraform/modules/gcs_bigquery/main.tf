resource "google_storage_bucket" "demo_bucket" {
    name                        = var.bucket_name
    location                    = "US"
    force_destroy               = true
    uniform_bucket_level_access = true
}

resource "google_bigquery_dataset" "demo_dataset" {
    dataset_id  = var.bq_dataset_name
    project     = var.project_id
    location    = var.region
}
