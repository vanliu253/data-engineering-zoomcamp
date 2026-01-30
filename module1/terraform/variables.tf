variable "location" {
  default = "US"
}

variable "credentials" {
  description = "My Credentials"
  default     = "./keys/dtc-de-484820-a234384f4dd2.json"
}

variable "bq_dataset_name" {
  description = "My BigQuery Dataset Name"
  default     = "demo_dataset"
}

variable "gcs_bucket_name" {
  description = "My Storage Bucket Name"
  default     = "dtc-de-484820-test-bucket"
}

variable "gcs_storage_class" {
  description = "Bucket Storage Class"
  default     = "STANDARD"
}

