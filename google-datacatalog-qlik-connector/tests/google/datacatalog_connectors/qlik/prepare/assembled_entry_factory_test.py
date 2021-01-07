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

import unittest
from unittest import mock

from google.datacatalog_connectors.qlik import prepare


class AssembledEntryFactoryTest(unittest.TestCase):
    __PREPARE_PACKAGE = 'google.datacatalog_connectors.qlik.prepare'
    __FACTORY_MODULE = f'{__PREPARE_PACKAGE}.assembled_entry_factory'

    @mock.patch(f'{__FACTORY_MODULE}.datacatalog_tag_factory'
                f'.DataCatalogTagFactory')
    @mock.patch(f'{__FACTORY_MODULE}.datacatalog_entry_factory'
                f'.DataCatalogEntryFactory')
    def setUp(self, mock_entry_factory, mock_tag_factory):
        self.__factory = prepare.AssembledEntryFactory(
            project_id='test-project',
            location_id='test-location',
            entry_group_id='test-entry-group',
            user_specified_system='test-system',
            site_url='https://test.server.com')

        self.__entry_factory = mock_entry_factory.return_value
        self.__tag_factory = mock_tag_factory.return_value

    def test_constructor_should_set_instance_attributes(self):
        attrs = self.__factory.__dict__

        self.assertEqual(
            self.__entry_factory,
            attrs['_AssembledEntryFactory__datacatalog_entry_factory'])

    def test_make_assembled_entry_for_custom_property_def_should_process_def(
            self):

        entry_factory = self.__entry_factory
        entry_factory.make_entry_for_custom_property_definition\
            .return_value = ('id', {})

        tag_templates_dict = {
            'qlik_custom_property_definition_metadata': {
                'name': 'tagTemplates/qlik_custom_property_def_metadata',
            }
        }

        assembled_entry = \
            self.__factory.make_assembled_entry_for_custom_property_def(
                {'id': 'test_definition'}, tag_templates_dict)

        entry_factory.make_entry_for_custom_property_definition\
            .assert_called_once()

        tags = assembled_entry.tags

        self.assertEqual(1, len(tags))
        self.__tag_factory.make_tag_for_custom_property_definition\
            .assert_called_once()

    def test_make_assembled_entries_for_stream_should_process_stream(self):
        entry_factory = self.__entry_factory
        entry_factory.make_entry_for_stream.return_value = ('id', {})

        tag_templates_dict = {
            'qlik_stream_metadata': {
                'name': 'tagTemplates/qlik_stream_metadata',
            }
        }

        assembled_entries = \
            self.__factory.make_assembled_entries_for_stream(
                self.__make_fake_stream(), tag_templates_dict)

        self.assertEqual(1, len(assembled_entries))
        entry_factory.make_entry_for_stream.assert_called_once()

        stream_assembled_entry = assembled_entries[0]
        tags = stream_assembled_entry.tags

        self.assertEqual(1, len(tags))
        self.__tag_factory.make_tag_for_stream.assert_called_once()

    def test_make_assembled_entries_for_stream_should_process_apps(self):
        entry_factory = self.__entry_factory
        entry_factory.make_entry_for_stream.return_value = ('id', {})
        entry_factory.make_entry_for_app.return_value = ('id', {})

        tag_templates_dict = {
            'qlik_app_metadata': {
                'name': 'tagTemplates/qlik_app_metadata',
            }
        }

        fake_stream = self.__make_fake_stream()
        fake_stream['apps'].append(self.__make_fake_app())
        assembled_entries = \
            self.__factory.make_assembled_entries_for_stream(
                fake_stream, tag_templates_dict)

        self.assertEqual(2, len(assembled_entries))
        entry_factory.make_entry_for_stream.assert_called_once()

        app_assembled_entry = assembled_entries[1]
        tags = app_assembled_entry.tags

        self.assertEqual(1, len(tags))
        self.__tag_factory.make_tag_for_app.assert_called_once()

    def test_make_assembled_entries_for_stream_should_process_dimensions(self):
        entry_factory = self.__entry_factory
        entry_factory.make_entry_for_stream.return_value = ('id', {})
        entry_factory.make_entry_for_app.return_value = ('id', {})
        entry_factory.make_entry_for_dimension.return_value = ('id', {})

        tag_templates_dict = {
            'qlik_dimension_metadata': {
                'name': 'tagTemplates/qlik_dimension_metadata',
            }
        }

        fake_stream = self.__make_fake_stream()
        fake_app = self.__make_fake_app()
        fake_app['dimensions'].append(self.__make_fake_dimension())
        fake_stream['apps'].append(fake_app)
        assembled_entries = \
            self.__factory.make_assembled_entries_for_stream(
                fake_stream, tag_templates_dict)

        self.assertEqual(3, len(assembled_entries))
        entry_factory.make_entry_for_dimension.assert_called_once()

        dimension_assembled_entry = assembled_entries[2]
        tags = dimension_assembled_entry.tags

        self.assertEqual(1, len(tags))
        self.__tag_factory.make_tag_for_dimension.assert_called_once()

    def test_make_assembled_entries_for_stream_should_process_measures(self):
        entry_factory = self.__entry_factory
        entry_factory.make_entry_for_stream.return_value = ('id', {})
        entry_factory.make_entry_for_app.return_value = ('id', {})
        entry_factory.make_entry_for_measure.return_value = ('id', {})

        tag_templates_dict = {
            'qlik_measure_metadata': {
                'name': 'tagTemplates/qlik_measure_metadata',
            }
        }

        fake_stream = self.__make_fake_stream()
        fake_app = self.__make_fake_app()
        fake_app['measures'].append(self.__make_fake_measure())
        fake_stream['apps'].append(fake_app)
        assembled_entries = \
            self.__factory.make_assembled_entries_for_stream(
                fake_stream, tag_templates_dict)

        self.assertEqual(3, len(assembled_entries))
        entry_factory.make_entry_for_measure.assert_called_once()

        measure_assembled_entry = assembled_entries[2]
        tags = measure_assembled_entry.tags

        self.assertEqual(1, len(tags))
        self.__tag_factory.make_tag_for_measure.assert_called_once()

    def test_make_assembled_entries_for_stream_should_process_sheets(self):
        entry_factory = self.__entry_factory
        entry_factory.make_entry_for_stream.return_value = ('id', {})
        entry_factory.make_entry_for_app.return_value = ('id', {})
        entry_factory.make_entry_for_sheet.return_value = ('id', {})

        tag_templates_dict = {
            'qlik_sheet_metadata': {
                'name': 'tagTemplates/qlik_sheet_metadata',
            }
        }

        fake_stream = self.__make_fake_stream()
        fake_app = self.__make_fake_app()
        fake_app['sheets'].append(self.__make_fake_sheet())
        fake_stream['apps'].append(fake_app)
        assembled_entries = \
            self.__factory.make_assembled_entries_for_stream(
                fake_stream, tag_templates_dict)

        self.assertEqual(3, len(assembled_entries))
        entry_factory.make_entry_for_sheet.assert_called_once()

        sheet_assembled_entry = assembled_entries[2]
        tags = sheet_assembled_entry.tags

        self.assertEqual(1, len(tags))
        self.__tag_factory.make_tag_for_sheet.assert_called_once()

    def test_make_assembled_entries_for_stream_should_process_visualizations(
            self):

        entry_factory = self.__entry_factory
        entry_factory.make_entry_for_stream.return_value = ('id', {})
        entry_factory.make_entry_for_app.return_value = ('id', {})
        entry_factory.make_entry_for_visualization.return_value = ('id', {})

        tag_templates_dict = {
            'qlik_visualization_metadata': {
                'name': 'tagTemplates/qlik_visualization_metadata',
            }
        }

        fake_stream = self.__make_fake_stream()
        fake_app = self.__make_fake_app()
        fake_app['visualizations'].append(self.__make_fake_visualization())
        fake_stream['apps'].append(fake_app)
        assembled_entries = \
            self.__factory.make_assembled_entries_for_stream(
                fake_stream, tag_templates_dict)

        self.assertEqual(3, len(assembled_entries))
        entry_factory.make_entry_for_visualization.assert_called_once()

        measure_assembled_entry = assembled_entries[2]
        tags = measure_assembled_entry.tags

        self.assertEqual(1, len(tags))
        self.__tag_factory.make_tag_for_visualization.assert_called_once()

    @classmethod
    def __make_fake_stream(cls):
        return {
            'id': 'test-stream',
            'name': 'Test stream',
            'apps': [],
        }

    @classmethod
    def __make_fake_app(cls):
        return {
            'id': 'test-app',
            'name': 'Test app',
            'dimensions': [],
            'measures': [],
            'visualizations': [],
            'sheets': [],
        }

    @classmethod
    def __make_fake_dimension(cls):
        return {
            'qInfo': {
                'qId': 'test-dimension',
            },
            'qMetaDef': {
                'title': 'Test dimension',
            },
        }

    @classmethod
    def __make_fake_measure(cls):
        return {
            'qInfo': {
                'qId': 'test-measure',
            },
            'qMetaDef': {
                'title': 'Test measure',
            },
        }

    @classmethod
    def __make_fake_sheet(cls):
        return {
            'qInfo': {
                'qId': 'test-sheet',
            },
            'qMeta': {
                'title': 'Test sheet',
            },
        }

    @classmethod
    def __make_fake_visualization(cls):
        return {
            'qInfo': {
                'qId': 'test-visualization',
            },
            'qMetaDef': {
                'title': 'Test visualization',
            },
        }
