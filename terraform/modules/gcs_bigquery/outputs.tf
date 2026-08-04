output "bucket_name" {
    description = "The name of the created GCS bucket"
    value       = google_storage_bucket.demo_bucket.name
}

output "bucket_url" {
    description = "The GCS URL of the bucket"
    value       = google_storage_bucket.demo_bucket.url
}

output "bq_dataset_id" {
    description = "The BigQuery dataset ID"
    value       = google_bigquery_dataset.demo_dataset.dataset_id
} 
