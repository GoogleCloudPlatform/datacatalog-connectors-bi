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

import re
import six
import unicodedata

from google.datacatalog_connectors.commons import prepare


class SisenseConnectorStringsHelper:
    """
    Temporary class providing the logic to format column names currently
    required by the Entry and Tag factories.

    TODO
     1. Merge this class with
     ``...datacatalog_connectors.commons.prepare.DataCatalogStringsHelper``.
     2. Delete this class.
     3. Refactor ``...commons.prepare.DataCatalogStringsHelper`` to turn
     ``normalize_string`` a public method.
     4. Refactor ``...commons.prepare.BaseEntryFactory`` to call
     ``...commons.prepare.DataCatalogStringsHelper.normalize_string`` instead
     of declaring its own __normalize_string method as it will be a duplicate.
     5. Refactor ``DataCatalogEntryFactory`` to call
     ``...commons.prepare.DataCatalogStringsHelper.normalize_string`` as the
     present class will no longer exist after the refactoring.
     6. Refactor ``DataCatalogTagFactory`` to call
     ``...commons.prepare.DataCatalogStringsHelper.normalize_string`` as the
     present class will no longer exist after the refactoring.
    """
    __ASCII_CHARACTER_ENCODING = 'ASCII'
    # Column names must contain only unicode characters excluding dots and
    # control characters and be at most 300 bytes long when encoded in UTF-8.
    __COLUMN_NAME_INVALID_CHARS_REGEX_PATTERN = r'[\.]+'
    __COLUMN_NAME_UTF8_MAX_LENGTH = 300

    @classmethod
    def format_column_name(cls, column_name, normalize=True):
        """
        Formats the column_name to fit the string bytes limit enforced by
        Data Catalog, and optionally normalizes it by applying a regex pattern
        that replaces unsupported characters with underscore.

        Warning: truncating and normalizing column names may lead to slightly
        different names from the source system columns.

        :param column_name: the value to be formatted.
        :param normalize: enables the normalize logic.

        :return: The formatted column name.
        """
        formatted_column_name = column_name
        if normalize:
            formatted_column_name = cls.__normalize_string(
                cls.__COLUMN_NAME_INVALID_CHARS_REGEX_PATTERN, column_name)

        return prepare.DataCatalogStringsHelper.truncate_string(
            formatted_column_name, cls.__COLUMN_NAME_UTF8_MAX_LENGTH)

    @classmethod
    def __normalize_string(cls, regex_pattern, source_string):
        formatted_str = re.sub(
            regex_pattern, '_',
            cls.__normalize_ascii_chars(source_string.strip()))

        # The __normalize_ascii_chars logic may replace a non ascii
        # char with space at the end of the string, so we need to do
        # an additional strip() to make sure it is removed from the
        # final normalized string.
        return formatted_str.strip()

    @classmethod
    def __normalize_ascii_chars(cls, source_string):
        encoding = cls.__ASCII_CHARACTER_ENCODING
        normalized = unicodedata.normalize(
            'NFKD', source_string
            if isinstance(source_string, six.string_types) else u'')
        encoded = normalized.encode(encoding, 'ignore')
        return encoded.decode()
