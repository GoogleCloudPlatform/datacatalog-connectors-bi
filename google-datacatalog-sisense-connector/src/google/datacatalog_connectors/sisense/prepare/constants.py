#!/usr/bin/python
#
# Copyright 2021 Google LLC
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

# This is the common prefix for all Sisense Entries.
ENTRY_ID_PREFIX = 'sss_'
# The below asset type specific strings are appended to the standard
# ENTRY_ID_PREFIX when generating Sisense Entry IDs.
ENTRY_ID_PART_DASHBOARD = 'db_'
ENTRY_ID_PART_FOLDER = 'fd_'
ENTRY_ID_PART_WIDGET = 'wg_'

# Name of the column used to store fields metadata in Data Catalog entries.
ENTRY_COLUMN_FIELDS = 'fields'
# Name of the column used to store metadata for nested JAQL Query filters in
# Data Catalog entries.
ENTRY_COLUMN_FILTER_BY = 'filterBy'
# Name of the column used to store filters metadata in Data Catalog entries.
ENTRY_COLUMN_FILTERS = 'filters'
# Name of the column used to store formula metadata in Data Catalog entries.
ENTRY_COLUMN_FORMULA = 'formula'

# The Sisense type for Dashboard assets.
SISENSE_ASSET_TYPE_DASHBOARD = 'dashboard'
# The Sisense type for Folder assets.
SISENSE_ASSET_TYPE_FOLDER = 'folder'

# The ID of the Tag Template created to store additional metadata for
# Dashboard-related Entries.
TAG_TEMPLATE_ID_DASHBOARD = 'sisense_dashboard_metadata'
# The ID of the Tag Template created to store additional metadata for
# Folder-related Entries.
TAG_TEMPLATE_ID_FOLDER = 'sisense_folder_metadata'
# The ID of the Tag Template created to store lineage metadata for
# JAQL-dependent entities.
TAG_TEMPLATE_ID_JAQL = 'sisense_jaql_metadata'
# The ID of the Tag Template created to store additional metadata for
# Widget-related Entries.
TAG_TEMPLATE_ID_WIDGET = 'sisense_widget_metadata'

# The user specified type of Dashboard-related Entries.
USER_SPECIFIED_TYPE_DASHBOARD = 'Dashboard'
# The user specified type of Folder-related Entries.
USER_SPECIFIED_TYPE_FOLDER = 'Folder'
# The user specified type of Widget-related Entries.
USER_SPECIFIED_TYPE_WIDGET = 'Widget'

# Name of the field used by Sisense Dashboards to store their filters.
DASHBOARD_FILTERS_FIELD_NAME = 'filters'
# Name of the field used by Sisense JAQL Queries to store their formula
# contexts.
JAQL_CONTEXT_FIELD_NAME = 'context'
# Name of the field used by Sisense JAQL Queries to store their filters.
JAQL_FILTER_FIELD_NAME = 'filter'
# Name of the field used by Sisense JAQL Query filters to store their own JAQL
# Queries.
JAQL_FILTER_BY_FIELD_NAME = 'by'
# Name of the field used by Sisense JAQL Queries to store their formulas.
JAQL_FORMULA_FIELD_NAME = 'formula'
# Name of the panel used by Sisense Widgets to store their filters.
WIDGET_FILTERS_PANEL_NAME = 'filters'
