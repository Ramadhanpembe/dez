output "bucket_name" {
    description = "The name of the created GCS bucket"
    value       = module.storage_and_bq.bucket_name
}

output "bucket_url" {
    description = "The GCS URL of the bucket"
    value       = module.storage_and_bq.bucket_url
}

output "bq_dataset_id" {
    description = "The BigQuery dataset ID"
    value       = module.storage_and_bq.bq_dataset_id
} 
