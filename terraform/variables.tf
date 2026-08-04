variable "project_id" {
    description = "The GCP Project ID"
    type        = string
}

variable "region" {
    description = "The GCP region for resources"
    type        = string
    default     = "us-central1"
}

variable "bucket_name" {
    description = "Globally unique name for the GCS bucket"
    type        = string
}

variable "bq_dataset_name" {
    description = "BigQuery dataset name"
    type        = string
    default     = "ny_taxi"
}