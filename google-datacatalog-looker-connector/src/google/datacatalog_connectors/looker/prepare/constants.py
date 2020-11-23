#!/usr/bin/python
#
# Copyright 2020 Google LLC
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

# This is the common prefix for all Looker Entries.
ENTRY_ID_PREFIX = 'lkr_'
# The below asset type specific strings are appended to the standard
# ENTRY_ID_PREFIX when generating Looker Entry IDs.
ENTRY_ID_PART_DASHBOARD = 'db_'
ENTRY_ID_PART_DASHBOARD_ELEMENT = 'de_'
ENTRY_ID_PART_FOLDER = 'fd_'
ENTRY_ID_PART_LOOK = 'lk_'
ENTRY_ID_PART_QUERY = 'qr_'

# The ID of the Tag Template created to store additional metadata for
# Dashboard-related Entries.
TAG_TEMPLATE_ID_DASHBOARD = 'looker_dashboard_metadata'
# The ID of the Tag Template created to store additional metadata for
# Dashboard Element-related Entries.
TAG_TEMPLATE_ID_DASHBOARD_ELEMENT = 'looker_dashboard_element_metadata'
# The ID of the Tag Template created to store additional metadata for
# Folder-related Entries.
TAG_TEMPLATE_ID_FOLDER = 'looker_folder_metadata'
# The ID of the Tag Template created to store additional metadata for
# Look-related Entries.
TAG_TEMPLATE_ID_LOOK = 'looker_look_metadata'
# The ID of the Tag Template created to store additional metadata for
# Query-related Entries.
TAG_TEMPLATE_ID_QUERY = 'looker_query_metadata'

# The user specified type of Dashboard-related Entries.
USER_SPECIFIED_TYPE_DASHBOARD = 'dashboard'
# The user specified type of Dashboard Element-related Entries.
USER_SPECIFIED_TYPE_DASHBOARD_ELEMENT = 'dashboard_element'
# The user specified type of Folder-related Entries.
USER_SPECIFIED_TYPE_FOLDER = 'folder'
# The user specified type of Look-related Entries.
USER_SPECIFIED_TYPE_LOOK = 'look'
# The user specified type of Query-related Entries.
USER_SPECIFIED_TYPE_QUERY = 'query'
