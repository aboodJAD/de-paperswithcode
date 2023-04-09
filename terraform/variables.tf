variable "project_id" {
  description = "Your GCP Project ID"
  type = string
}

variable "region" {
  description = "Region for GCP resources. Choose as per your location: https://cloud.google.com/about/locations"
  type        = string
}

variable "datalake_bucket_name" {type = string}

variable "bq_dataset_name" {type = string}

variable "dataproc_cluster_name" {type = string}

variable "dataproc_temp_bucket" {type = string}
