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

ENTRY_ID_LENGTH = 64
ENTRY_ID_PREFIX = 'qlik_'
ENTRY_ID_APP = 'app_'
ENTRY_ID_STREAM = 'str_'
NO_PREFIX_ENTRY_ID_LENGTH = ENTRY_ID_LENGTH - len(ENTRY_ID_PREFIX)

TAG_TEMPLATE_ID_APP = 'qlik_app_metadata'
TAG_TEMPLATE_ID_STREAM = 'qlik_stream_metadata'

USER_SPECIFIED_TYPE_APP = 'app'
USER_SPECIFIED_TYPE_STREAM = 'stream'
