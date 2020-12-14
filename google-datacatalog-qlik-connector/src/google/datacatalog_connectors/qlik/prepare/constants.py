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

# The max length of a Data Catalog Entry ID.
ENTRY_ID_MAX_LENGTH = 64
# The below asset type specific strings are appended to the standard
# ENTRY_ID_PREFIX when generating Qlik Entry IDs.
ENTRY_ID_PART_APP = 'app_'
ENTRY_ID_PART_CUSTOM_PROPERTY_DEFINITION = 'cpd_'
ENTRY_ID_PART_SHEET = 'sht_'
ENTRY_ID_PART_STREAM = 'str_'
# This is the common prefix for all Qlik Entries.
ENTRY_ID_PREFIX = 'qlik_'
# This constant represents the max length a generated Entry ID can have before
# appending the standard ENTRY_ID_PREFIX to it. The necessary right-most chars
# will be removed from a generated Entry ID if it has more chars than the value
# of this constant.
NO_PREFIX_ENTRY_ID_MAX_LENGTH = ENTRY_ID_MAX_LENGTH - len(ENTRY_ID_PREFIX)

# The ID of the Tag Template created to store additional metadata for the
# App-related Entries.
TAG_TEMPLATE_ID_APP = 'qlik_app_metadata'
# The ID of the Tag Template created to store additional metadata for the
# Custom Property Definition related Entries.
TAG_TEMPLATE_ID_CUSTOM_PROPERTY_DEFINITION = \
    'qlik_custom_property_definition_metadata'
# Prefix for IDs of the Tag Templates created to tag Entries with their
# Custom Properties.
TAG_TEMPLATE_ID_PREFIX_CUSTOM_PROPERTY = 'qlik_cp__'
# The ID of the Tag Template created to store additional metadata for the
# Sheet-related Entries.
TAG_TEMPLATE_ID_SHEET = 'qlik_sheet_metadata'
# The ID of the Tag Template created to store additional metadata for the
# Stream-related Entries.
TAG_TEMPLATE_ID_STREAM = 'qlik_stream_metadata'

# The user specified type of the App-related Entries.
USER_SPECIFIED_TYPE_APP = 'app'
# The user specified type of the Custom Property Definition related Entries.
USER_SPECIFIED_TYPE_CUSTOM_PROPERTY_DEFINITION = 'custom_property_definition'
# The user specified type of the Sheet-related Entries.
USER_SPECIFIED_TYPE_SHEET = 'sheet'
# The user specified type of the Stream-related Entries.
USER_SPECIFIED_TYPE_STREAM = 'stream'
