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

import re
import six
import unicodedata

from google.datacatalog_connectors.qlik.prepare import constants


class DynamicPropertiesHelper:
    __ASCII_CHARACTER_ENCODING = 'ASCII'

    @classmethod
    def make_display_name_for_custom_property_value_tag_template(
            cls, definition_metadata, value):

        display_name = f'Qlik Custom Property' \
                       f' - {definition_metadata.get("name")} - {value}'
        return cls.normalize_tag_template_display_name(display_name)

    @classmethod
    def make_id_for_custom_property_value_tag_template(cls,
                                                       definition_metadata,
                                                       value):

        definition_id = definition_metadata.get("id")
        definition_id_start = definition_id[:definition_id.find('-')]
        generated_id = f'{constants.TAG_TEMPLATE_ID_PREFIX_CUSTOM_PROPERTY}' \
                       f'{definition_id_start}__{value}'
        return cls.normalize_tag_template_id(generated_id)

    @classmethod
    def normalize_tag_template_display_name(cls, source_name):
        formatted_name = re.sub(
            r'[^\w\- ]+', '_',
            cls.__normalize_ascii_chars(source_name).strip())
        return formatted_name

    @classmethod
    def normalize_tag_template_id(cls, source_id):
        lower_case_id = source_id.lower().strip() if source_id else ''
        formatted_id = re.sub(
            r'[^a-z0-9_]+', '_',
            cls.__normalize_ascii_chars(lower_case_id.strip()))
        return formatted_id[:64] if len(formatted_id) > 64 else formatted_id

    @classmethod
    def __normalize_ascii_chars(cls, source_string):
        encoding = cls.__ASCII_CHARACTER_ENCODING
        normalized = unicodedata.normalize(
            'NFKD', source_string
            if isinstance(source_string, six.string_types) else u'')
        encoded = normalized.encode(encoding, 'ignore')
        return encoded.decode()
