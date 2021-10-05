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

from typing import List, Optional, Tuple

from google.cloud import datacatalog
from google.cloud.datacatalog import Entry, SearchCatalogResult, Tag


def make_fake_search_result(entry_id: str) -> SearchCatalogResult:
    result = datacatalog.SearchCatalogResult()
    result.relative_resource_name = f'fake_entries/{entry_id}'
    return result


def make_fake_entry(entry_id: str) -> Entry:
    entry = datacatalog.Entry()
    entry.name = f'fake_entries/{entry_id}'
    entry.schema = datacatalog.Schema()
    return entry


def make_fake_tag(template: Optional[str] = 'template',
                  column: Optional[str] = None,
                  string_fields: List[Tuple[str, str]] = None) -> Tag:

    tag = datacatalog.Tag()
    tag.template = template

    if column:
        tag.column = column

    if string_fields:
        for string_field in string_fields:
            tag_field = datacatalog.TagField()
            tag_field.string_value = string_field[1]
            tag.fields[string_field[0]] = tag_field

    return tag
