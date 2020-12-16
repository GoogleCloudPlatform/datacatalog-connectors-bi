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

import unittest
from unittest import mock

from google.datacatalog_connectors.qlik.prepare import \
    dynamic_properties_helper as dph


class DynamicPropertiesHelperTest(unittest.TestCase):
    __PREPARE_PACKAGE = 'google.datacatalog_connectors.qlik.prepare'
    __HELPER_MODULE = f'{__PREPARE_PACKAGE}.dynamic_properties_helper'

    @mock.patch(f'{__PREPARE_PACKAGE}.datacatalog_tag_template_factory.dph'
                f'.DynamicPropertiesHelper'
                f'.normalize_tag_template_display_name')
    def test_make_display_name_for_custom_property_value_tag_template_should_concat_fields(  # noqa E510
            self, mock_normalize):

        dph.DynamicPropertiesHelper\
            .make_display_name_for_custom_property_value_tag_template(
                {'name': 'Test definition'}, 'Value 1')

        mock_normalize.assert_called_with(
            'Qlik Custom Property - Test definition - Value 1')

    @mock.patch(f'{__PREPARE_PACKAGE}.datacatalog_tag_template_factory.dph'
                f'.DynamicPropertiesHelper'
                f'.normalize_tag_template_display_name')
    def test_make_display_name_for_custom_property_value_tag_template_should_format_name(  # noqa E510
            self, mock_normalize):

        dph.DynamicPropertiesHelper \
            .make_display_name_for_custom_property_value_tag_template(
                {}, 'Value 1')

        mock_normalize.assert_called_once()

    @mock.patch(f'{__PREPARE_PACKAGE}.datacatalog_tag_template_factory.dph'
                f'.DynamicPropertiesHelper.normalize_tag_template_id')
    def test_make_id_for_custom_property_value_tag_template_should_concat_fields(  # noqa E510
            self, mock_normalize):

        dph.DynamicPropertiesHelper\
            .make_id_for_custom_property_value_tag_template(
                {'id': 'a123-b456'}, 'Value 1')

        mock_normalize.assert_called_with('qlik_cp__a123__Value 1')

    @mock.patch(f'{__PREPARE_PACKAGE}.datacatalog_tag_template_factory.dph'
                f'.DynamicPropertiesHelper.normalize_tag_template_id')
    def test_make_id_for_custom_property_value_tag_template_should_format_id(
            self, mock_normalize):

        dph.DynamicPropertiesHelper\
            .make_id_for_custom_property_value_tag_template(
                {'id': 'a123-b456'}, 'Value 1')

        mock_normalize.assert_called_once()

    def test_normalize_tag_template_display_name_should_replace_invalid_chars(
            self):

        normalized_name = dph.DynamicPropertiesHelper\
            .normalize_tag_template_display_name('Test definition :: Value 1')

        self.assertEqual('Test definition _ Value 1', normalized_name)

    def test_normalize_tag_template_id_should_replace_invalid_chars(self):

        normalized_id = dph.DynamicPropertiesHelper \
            .normalize_tag_template_id('qlik_cp__a123__Value 1')

        self.assertEqual('qlik_cp__a123__value_1', normalized_id)
