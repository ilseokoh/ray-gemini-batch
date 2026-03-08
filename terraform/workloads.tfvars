# Copyright 2023 Google LLC
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.


##common variables
## Need to pull this variables from tf output from previous platform stage
project_id = "kevin-ai-playground"

## This is required for terraform to connect to GKE cluster and deploy workloads.
cluster_name     = "ray-enabled-gke"
cluster_location = "asia-northeast3"

## If terraform should create a new GKE cluster, fill in this section as well.
##    By default, a public autopilot GKE cluster will be created in the default network.
##    Set the autopilot_cluster variable to false to create a standard cluster instead.
create_cluster    = false
autopilot_cluster = true

#######################################################
####    APPLICATIONS
#######################################################

## GKE environment variables
kubernetes_namespace = "default"

# Creates a google service account & k8s service account & configures workload identity with appropriate permissions.
# Set to false & update the variable `workload_identity_service_account` to use an existing IAM service account.
create_service_account            = true
workload_identity_service_account = "ray-sa"

# Bucket name should be globally unique.
create_gcs_bucket               = true
gcs_bucket                      = "ray-bucket-zydg1"
create_ray_cluster              = true
ray_cluster_name                = "ray-cluster"
enable_grafana_on_ray_dashboard = false
enable_gpu                      = false

ray_dashboard_add_auth          = false