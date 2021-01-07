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

from google.cloud import datacatalog
from google.datacatalog_connectors.commons import prepare

from google.datacatalog_connectors.qlik import sync


class MetadataSynchronizerTest(unittest.TestCase):
    __SYNC_PACKAGE = 'google.datacatalog_connectors.qlik.sync'
    __SYNCR_MODULE = f'{__SYNC_PACKAGE}.metadata_synchronizer'

    @mock.patch(f'{__SYNCR_MODULE}.prepare.AssembledEntryFactory',
                lambda *args, **kwargs: mock.MagicMock())
    @mock.patch(f'{__SYNCR_MODULE}.scrape.MetadataScraper',
                lambda *args, **kwargs: mock.MagicMock())
    def setUp(self):
        self.__synchronizer = sync.MetadataSynchronizer(
            qlik_server_address='test-server',
            qlik_ad_domain='test-domain',
            qlik_username='test-username',
            qlik_password='test-password',
            datacatalog_project_id='test-project-id',
            datacatalog_location_id='test-location-id')

    def test_constructor_should_set_instance_attributes(self):
        attrs = self.__synchronizer.__dict__

        self.assertEqual('test-project-id',
                         attrs['_MetadataSynchronizer__project_id'])
        self.assertEqual('test-location-id',
                         attrs['_MetadataSynchronizer__location_id'])
        self.assertIsNotNone(attrs['_MetadataSynchronizer__metadata_scraper'])
        self.assertIsNotNone(
            attrs['_MetadataSynchronizer__tag_template_factory'])
        self.assertEqual('test-server',
                         attrs['_MetadataSynchronizer__site_url'])
        self.assertIsNotNone(
            attrs['_MetadataSynchronizer__assembled_entry_factory'])

    @mock.patch(f'{__SYNCR_MODULE}.ingest.DataCatalogMetadataIngestor')
    @mock.patch(f'{__SYNCR_MODULE}.cleanup.DataCatalogMetadataCleaner')
    @mock.patch(f'{__SYNCR_MODULE}.prepare.EntryRelationshipMapper',
                lambda *args: mock.MagicMock())
    def test_run_no_metadata_should_clean_but_not_ingest_metadata(
            self, mock_cleaner, mock_ingestor):

        scraper = self.__synchronizer.__dict__[
            '_MetadataSynchronizer__metadata_scraper']

        self.__synchronizer.run()

        scraper.scrape_all_custom_property_definitions.assert_called_once()
        scraper.scrape_all_streams.assert_called_once()

        cleaner = mock_cleaner.return_value
        cleaner.delete_obsolete_metadata.assert_called_once()

        ingestor = mock_ingestor.return_value
        ingestor.ingest_metadata.assert_not_called()

    @mock.patch(f'{__SYNCR_MODULE}.ingest.DataCatalogMetadataIngestor')
    @mock.patch(f'{__SYNCR_MODULE}.cleanup.DataCatalogMetadataCleaner')
    @mock.patch(f'{__SYNCR_MODULE}.prepare.EntryRelationshipMapper')
    def test_run_custom_property_def_should_traverse_main_workflow_steps(
            self, mock_mapper, mock_cleaner, mock_ingestor):

        attrs = self.__synchronizer.__dict__
        scraper = attrs['_MetadataSynchronizer__metadata_scraper']
        assembled_entry_factory = attrs[
            '_MetadataSynchronizer__assembled_entry_factory']

        scraper.scrape_all_custom_property_definitions.return_value = [{
            'id': 'test-def',
        }]
        assembled_entry_factory.make_assembled_entry_for_custom_property_def\
            .return_value = prepare.AssembledEntryData(
                'test-def',
                self.__make_fake_entry('custom_property_definition'),
                [])

        self.__synchronizer.run()

        expected_make_assembled_entries_call_arg = {
            'id': 'test-def',
        }

        actual_call_args = assembled_entry_factory\
            .make_assembled_entry_for_custom_property_def.call_args[0]
        self.assertEqual(expected_make_assembled_entries_call_arg,
                         actual_call_args[0])

        mapper = mock_mapper.return_value
        mapper.fulfill_tag_fields.assert_called_once()

        cleaner = mock_cleaner.return_value
        cleaner.delete_obsolete_metadata.assert_called_once()

        ingestor = mock_ingestor.return_value
        ingestor.ingest_metadata.assert_called_once()

    @mock.patch(f'{__SYNCR_MODULE}.prepare.DataCatalogTagTemplateFactory'
                f'.make_tag_template_for_custom_property_value')
    @mock.patch(f'{__SYNCR_MODULE}.ingest.DataCatalogMetadataIngestor',
                lambda *args: mock.MagicMock())
    @mock.patch(f'{__SYNCR_MODULE}.cleanup.DataCatalogMetadataCleaner',
                lambda *args: mock.MagicMock())
    @mock.patch(f'{__SYNCR_MODULE}.prepare.EntryRelationshipMapper',
                lambda *args: mock.MagicMock())
    def test_run_custom_property_def_should_make_template_for_choice_value(
            self, mock_make_tag_template_for_custom_property_value):

        mock_make_tag_template_for_custom_property_value.return_value.name = \
            'parent/tagTemplates/def__value_1'

        attrs = self.__synchronizer.__dict__
        scraper = attrs['_MetadataSynchronizer__metadata_scraper']

        scraper.scrape_all_custom_property_definitions.return_value = [{
            'id': 'test-def',
            'choiceValues': ['Value 1']
        }]

        self.__synchronizer.run()

        mock_make_tag_template_for_custom_property_value.assert_called_once()

    @mock.patch(f'{__SYNCR_MODULE}.ingest.DataCatalogMetadataIngestor')
    @mock.patch(f'{__SYNCR_MODULE}.cleanup.DataCatalogMetadataCleaner')
    @mock.patch(f'{__SYNCR_MODULE}.prepare.EntryRelationshipMapper')
    def test_run_stream_metadata_should_traverse_main_workflow_steps(
            self, mock_mapper, mock_cleaner, mock_ingestor):

        attrs = self.__synchronizer.__dict__
        scraper = attrs['_MetadataSynchronizer__metadata_scraper']
        assembled_entry_factory = attrs[
            '_MetadataSynchronizer__assembled_entry_factory']

        scraper.scrape_all_streams.return_value = [self.__make_fake_stream()]
        assembled_entry_factory.make_assembled_entries_for_stream\
            .return_value = [prepare.AssembledEntryData(
                'test-stream', self.__make_fake_entry('stream'), [])]

        self.__synchronizer.run()

        expected_make_assembled_entries_call_arg = {
            'id': 'test-stream',
        }

        actual_call_args = assembled_entry_factory\
            .make_assembled_entries_for_stream.call_args[0]
        self.assertEqual(expected_make_assembled_entries_call_arg,
                         actual_call_args[0])

        mapper = mock_mapper.return_value
        mapper.fulfill_tag_fields.assert_called_once()

        cleaner = mock_cleaner.return_value
        cleaner.delete_obsolete_metadata.assert_called_once()

        ingestor = mock_ingestor.return_value
        ingestor.ingest_metadata.assert_called_once()

    @mock.patch(f'{__SYNCR_MODULE}.ingest.DataCatalogMetadataIngestor')
    @mock.patch(f'{__SYNCR_MODULE}.cleanup.DataCatalogMetadataCleaner',
                lambda *args: mock.MagicMock())
    @mock.patch(f'{__SYNCR_MODULE}.prepare.EntryRelationshipMapper',
                lambda *args: mock.MagicMock())
    def test_run_stream_metadata_should_process_only_required_template(
            self, mock_ingestor):

        attrs = self.__synchronizer.__dict__
        scraper = attrs['_MetadataSynchronizer__metadata_scraper']
        assembled_entry_factory = attrs[
            '_MetadataSynchronizer__assembled_entry_factory']

        scraper.scrape_all_streams.return_value = [self.__make_fake_stream()]
        fake_entry = self.__make_fake_entry('stream')
        fake_tag = self.__make_fake_tag('projects/test-project-id'
                                        '/locations/test-location-id'
                                        '/tagTemplates/qlik_stream_metadata')
        assembled_entry_factory.make_assembled_entries_for_stream\
            .return_value = [prepare.AssembledEntryData(
                'test-stream', fake_entry, [fake_tag])]

        self.__synchronizer.run()

        ingest_metadata_call_args = \
            mock_ingestor.return_value.ingest_metadata.call_args[0]
        templates_dict_call_arg = ingest_metadata_call_args[1]

        self.assertEqual(1, len(templates_dict_call_arg))
        self.assertTrue('qlik_stream_metadata' in templates_dict_call_arg)

    @mock.patch(f'{__SYNCR_MODULE}.ingest.DataCatalogMetadataIngestor',
                lambda *args: mock.MagicMock())
    @mock.patch(f'{__SYNCR_MODULE}.cleanup.DataCatalogMetadataCleaner',
                lambda *args: mock.MagicMock())
    @mock.patch(f'{__SYNCR_MODULE}.prepare.EntryRelationshipMapper',
                lambda *args: mock.MagicMock())
    def test_run_published_app_should_properly_ask_assembled_entries(self):
        attrs = self.__synchronizer.__dict__
        scraper = attrs['_MetadataSynchronizer__metadata_scraper']
        assembled_entry_factory = attrs[
            '_MetadataSynchronizer__assembled_entry_factory']

        scraper.scrape_all_streams.return_value = [self.__make_fake_stream()]
        scraper.scrape_all_apps.return_value = \
            [self.__make_fake_published_app()]
        scraper.scrape_dimensions.return_value = []
        scraper.scrape_measures.return_value = []
        scraper.scrape_visualizations.return_value = []
        scraper.scrape_sheets.return_value = []

        self.__synchronizer.run()

        expected_make_assembled_entries_call_arg = {
            'id':
                'test-stream',
            'apps': [{
                'id': 'test-app',
                'published': True,
                'stream': {
                    'id': 'test-stream'
                },
                'dimensions': [],
                'measures': [],
                'visualizations': [],
                'sheets': [],
            }]
        }

        actual_call_args = assembled_entry_factory\
            .make_assembled_entries_for_stream.call_args[0]
        self.assertEqual(expected_make_assembled_entries_call_arg,
                         actual_call_args[0])

    @mock.patch(f'{__SYNCR_MODULE}.ingest.DataCatalogMetadataIngestor',
                lambda *args: mock.MagicMock())
    @mock.patch(f'{__SYNCR_MODULE}.cleanup.DataCatalogMetadataCleaner',
                lambda *args: mock.MagicMock())
    @mock.patch(f'{__SYNCR_MODULE}.prepare.EntryRelationshipMapper',
                lambda *args: mock.MagicMock())
    def test_run_not_published_app_should_properly_ask_assembled_entries(self):
        attrs = self.__synchronizer.__dict__
        scraper = attrs['_MetadataSynchronizer__metadata_scraper']
        assembled_entry_factory = attrs[
            '_MetadataSynchronizer__assembled_entry_factory']

        scraper.scrape_all_streams.return_value = [self.__make_fake_stream()]
        scraper.scrape_all_apps.return_value = [self.__make_fake_wip_app()]
        assembled_entry_factory.make_assembled_entries_for_stream\
            .return_value = [
                prepare.AssembledEntryData(
                    'test-stream', self.__make_fake_entry('stream'), [])
            ]

        self.__synchronizer.run()

        expected_make_assembled_entries_call_arg = {
            'id': 'test-stream',
        }

        actual_call_args = assembled_entry_factory\
            .make_assembled_entries_for_stream.call_args[0]
        self.assertEqual(expected_make_assembled_entries_call_arg,
                         actual_call_args[0])

    @mock.patch(f'{__SYNCR_MODULE}.ingest.DataCatalogMetadataIngestor',
                lambda *args: mock.MagicMock())
    @mock.patch(f'{__SYNCR_MODULE}.cleanup.DataCatalogMetadataCleaner',
                lambda *args: mock.MagicMock())
    @mock.patch(f'{__SYNCR_MODULE}.prepare.EntryRelationshipMapper',
                lambda *args: mock.MagicMock())
    def test_run_dimension_should_properly_ask_assembled_entries(self):
        attrs = self.__synchronizer.__dict__
        scraper = attrs['_MetadataSynchronizer__metadata_scraper']
        assembled_entry_factory = attrs[
            '_MetadataSynchronizer__assembled_entry_factory']

        scraper.scrape_all_streams.return_value = [self.__make_fake_stream()]
        scraper.scrape_all_apps.return_value = \
            [self.__make_fake_published_app()]
        scraper.scrape_dimensions.return_value = [self.__make_fake_dimension()]
        scraper.scrape_measures.return_value = []
        scraper.scrape_visualizations.return_value = []
        scraper.scrape_sheets.return_value = []

        self.__synchronizer.run()

        expected_make_assembled_entries_call_arg = {
            'id':
                'test-stream',
            'apps': [{
                'id': 'test-app',
                'published': True,
                'stream': {
                    'id': 'test-stream'
                },
                'dimensions': [{
                    'qInfo': {
                        'qId': 'test-dimension',
                    },
                    'qDim': {},
                    'qMetaDef': {},
                    'app': {
                        'id': 'test-app',
                        'name': None
                    },
                }],
                'measures': [],
                'visualizations': [],
                'sheets': [],
            }]
        }

        actual_call_args = assembled_entry_factory\
            .make_assembled_entries_for_stream.call_args[0]
        self.assertEqual(expected_make_assembled_entries_call_arg,
                         actual_call_args[0])

    @mock.patch(f'{__SYNCR_MODULE}.ingest.DataCatalogMetadataIngestor',
                lambda *args: mock.MagicMock())
    @mock.patch(f'{__SYNCR_MODULE}.cleanup.DataCatalogMetadataCleaner',
                lambda *args: mock.MagicMock())
    @mock.patch(f'{__SYNCR_MODULE}.prepare.EntryRelationshipMapper',
                lambda *args: mock.MagicMock())
    def test_run_measure_should_properly_ask_assembled_entries(self):
        attrs = self.__synchronizer.__dict__
        scraper = attrs['_MetadataSynchronizer__metadata_scraper']
        assembled_entry_factory = attrs[
            '_MetadataSynchronizer__assembled_entry_factory']

        scraper.scrape_all_streams.return_value = [self.__make_fake_stream()]
        scraper.scrape_all_apps.return_value = \
            [self.__make_fake_published_app()]
        scraper.scrape_dimensions.return_value = []
        scraper.scrape_measures.return_value = [self.__make_fake_measure()]
        scraper.scrape_visualizations.return_value = []
        scraper.scrape_sheets.return_value = []

        self.__synchronizer.run()

        expected_make_assembled_entries_call_arg = {
            'id':
                'test-stream',
            'apps': [{
                'id': 'test-app',
                'published': True,
                'stream': {
                    'id': 'test-stream'
                },
                'dimensions': [],
                'measures': [{
                    'qInfo': {
                        'qId': 'test-measure',
                    },
                    'qMeasure': {},
                    'qMetaDef': {},
                    'app': {
                        'id': 'test-app',
                        'name': None
                    },
                }],
                'visualizations': [],
                'sheets': [],
            }]
        }

        actual_call_args = assembled_entry_factory\
            .make_assembled_entries_for_stream.call_args[0]
        self.assertEqual(expected_make_assembled_entries_call_arg,
                         actual_call_args[0])

    @mock.patch(f'{__SYNCR_MODULE}.ingest.DataCatalogMetadataIngestor',
                lambda *args: mock.MagicMock())
    @mock.patch(f'{__SYNCR_MODULE}.cleanup.DataCatalogMetadataCleaner',
                lambda *args: mock.MagicMock())
    @mock.patch(f'{__SYNCR_MODULE}.prepare.EntryRelationshipMapper',
                lambda *args: mock.MagicMock())
    def test_run_visualization_should_properly_ask_assembled_entries(self):
        attrs = self.__synchronizer.__dict__
        scraper = attrs['_MetadataSynchronizer__metadata_scraper']
        assembled_entry_factory = attrs[
            '_MetadataSynchronizer__assembled_entry_factory']

        scraper.scrape_all_streams.return_value = [self.__make_fake_stream()]
        scraper.scrape_all_apps.return_value = \
            [self.__make_fake_published_app()]
        scraper.scrape_dimensions.return_value = []
        scraper.scrape_measures.return_value = []
        scraper.scrape_visualizations.return_value = [
            self.__make_fake_visualization()
        ]
        scraper.scrape_sheets.return_value = []

        self.__synchronizer.run()

        expected_make_assembled_entries_call_arg = {
            'id':
                'test-stream',
            'apps': [{
                'id': 'test-app',
                'published': True,
                'stream': {
                    'id': 'test-stream'
                },
                'dimensions': [],
                'measures': [],
                'visualizations': [{
                    'qInfo': {
                        'qId': 'test-visualization',
                    },
                    'qMetaDef': {},
                    'app': {
                        'id': 'test-app',
                        'name': None
                    },
                }],
                'sheets': [],
            }]
        }

        actual_call_args = assembled_entry_factory\
            .make_assembled_entries_for_stream.call_args[0]
        self.assertEqual(expected_make_assembled_entries_call_arg,
                         actual_call_args[0])

    @mock.patch(f'{__SYNCR_MODULE}.ingest.DataCatalogMetadataIngestor',
                lambda *args: mock.MagicMock())
    @mock.patch(f'{__SYNCR_MODULE}.cleanup.DataCatalogMetadataCleaner',
                lambda *args: mock.MagicMock())
    @mock.patch(f'{__SYNCR_MODULE}.prepare.EntryRelationshipMapper',
                lambda *args: mock.MagicMock())
    def test_run_published_sheet_should_properly_ask_assembled_entries(self):
        attrs = self.__synchronizer.__dict__
        scraper = attrs['_MetadataSynchronizer__metadata_scraper']
        assembled_entry_factory = attrs[
            '_MetadataSynchronizer__assembled_entry_factory']

        scraper.scrape_all_streams.return_value = [self.__make_fake_stream()]
        scraper.scrape_all_apps.return_value = \
            [self.__make_fake_published_app()]
        scraper.scrape_dimensions.return_value = []
        scraper.scrape_measures.return_value = []
        scraper.scrape_visualizations.return_value = []
        scraper.scrape_sheets.return_value = [
            self.__make_fake_published_sheet()
        ]

        self.__synchronizer.run()

        expected_make_assembled_entries_call_arg = {
            'id':
                'test-stream',
            'apps': [{
                'id':
                    'test-app',
                'published':
                    True,
                'stream': {
                    'id': 'test-stream'
                },
                'dimensions': [],
                'measures': [],
                'visualizations': [],
                'sheets': [{
                    'qInfo': {
                        'qId': 'test-sheet',
                    },
                    'qMeta': {
                        'published': True,
                    },
                    'app': {
                        'id': 'test-app',
                        'name': None
                    },
                }],
            }]
        }

        actual_call_args = assembled_entry_factory\
            .make_assembled_entries_for_stream.call_args[0]
        self.assertEqual(expected_make_assembled_entries_call_arg,
                         actual_call_args[0])

    @mock.patch(f'{__SYNCR_MODULE}.ingest.DataCatalogMetadataIngestor',
                lambda *args: mock.MagicMock())
    @mock.patch(f'{__SYNCR_MODULE}.cleanup.DataCatalogMetadataCleaner',
                lambda *args: mock.MagicMock())
    @mock.patch(f'{__SYNCR_MODULE}.prepare.EntryRelationshipMapper',
                lambda *args: mock.MagicMock())
    def test_run_not_published_sheet_should_properly_ask_assembled_entries(
            self):

        attrs = self.__synchronizer.__dict__
        scraper = attrs['_MetadataSynchronizer__metadata_scraper']
        assembled_entry_factory = attrs[
            '_MetadataSynchronizer__assembled_entry_factory']

        scraper.scrape_all_streams.return_value = [self.__make_fake_stream()]
        scraper.scrape_all_apps.return_value = \
            [self.__make_fake_published_app()]
        scraper.scrape_dimensions.return_value = []
        scraper.scrape_measures.return_value = []
        scraper.scrape_visualizations.return_value = []
        scraper.scrape_sheets.return_value = [self.__make_fake_wip_sheet()]

        self.__synchronizer.run()

        expected_make_assembled_entries_call_arg = {
            'id':
                'test-stream',
            'apps': [{
                'id': 'test-app',
                'published': True,
                'stream': {
                    'id': 'test-stream'
                },
                'dimensions': [],
                'measures': [],
                'visualizations': [],
                'sheets': [],
            }]
        }

        actual_call_args = assembled_entry_factory\
            .make_assembled_entries_for_stream.call_args[0]
        self.assertEqual(expected_make_assembled_entries_call_arg,
                         actual_call_args[0])

    @classmethod
    def __make_fake_stream(cls):
        return {
            'id': 'test-stream',
        }

    @classmethod
    def __make_fake_published_app(cls):
        return {
            'id': 'test-app',
            'published': True,
            'stream': cls.__make_fake_stream(),
        }

    @classmethod
    def __make_fake_wip_app(cls):
        return {
            'id': 'test-app',
            'published': False,
        }

    @classmethod
    def __make_fake_dimension(cls):
        return {
            'qInfo': {
                'qId': 'test-dimension',
            },
            'qDim': {},
            'qMetaDef': {},
        }

    @classmethod
    def __make_fake_measure(cls):
        return {
            'qInfo': {
                'qId': 'test-measure',
            },
            'qMeasure': {},
            'qMetaDef': {},
        }

    @classmethod
    def __make_fake_visualization(cls):
        return {
            'qInfo': {
                'qId': 'test-visualization',
            },
            'qMetaDef': {},
        }

    @classmethod
    def __make_fake_sheet(cls):
        return {
            'qInfo': {
                'qId': 'test-sheet',
            },
            'qMeta': {},
        }

    @classmethod
    def __make_fake_published_sheet(cls):
        sheet = cls.__make_fake_sheet()
        sheet['qMeta']['published'] = True
        return sheet

    @classmethod
    def __make_fake_wip_sheet(cls):
        sheet = cls.__make_fake_sheet()
        sheet['qMeta']['published'] = False
        return sheet

    @classmethod
    def __make_fake_entry(cls, user_specified_type):
        entry = datacatalog.Entry()
        entry.user_specified_type = user_specified_type
        return entry

    @classmethod
    def __make_fake_tag(cls, template_name):
        tag = datacatalog.Tag()
        tag.template = template_name
        return tag
