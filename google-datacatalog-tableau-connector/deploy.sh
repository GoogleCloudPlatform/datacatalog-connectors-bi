#!/bin/bash -eu
#
# Copyright 2019 Google LLC
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

#!/usr/bin/env bash

gcloud builds submit --config=cloudbuild.hook.yaml --substitutions=_SERVICE_NAME="tableau-hook-sync",TAG_NAME="v0.0.1",_ENV_PROJECT_ID="TABLEAU2DC_DATACATALOG_PROJECT_ID=${TABLEAU2DC_DATACATALOG_PROJECT_ID}",_ENV_LOCATION_ID="TABLEAU2DC_DATACATALOG_LOCATION_ID=${TABLEAU2DC_DATACATALOG_LOCATION_ID}",_ENV_TABLEAU_SITE="TABLEAU2DC_TABLEAU_SITE=${TABLEAU2DC_TABLEAU_SITE}",_ENV_TABLEAU_PASSWORD="TABLEAU2DC_TABLEAU_PASSWORD=${TABLEAU2DC_TABLEAU_PASSWORD}",_ENV_TABLEAU_USERNAME="TABLEAU2DC_TABLEAU_USERNAME=${TABLEAU2DC_TABLEAU_USERNAME}",_ENV_TABLEAU_API_VERSION="TABLEAU2DC_TABLEAU_API_VERSION=${TABLEAU2DC_TABLEAU_API_VERSION}",_ENV_TABLEAU_SERVER="TABLEAU2DC_TABLEAU_SERVER=${TABLEAU2DC_TABLEAU_SERVER}",_ENV_TABLEAU_API_KEY="TABLEAU2DC_API_KEY=${TABLEAU2DC_API_KEY}"