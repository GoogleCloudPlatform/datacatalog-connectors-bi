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

from google.cloud import datacatalog
from google.datacatalog_connectors.commons import prepare

from google.datacatalog_connectors.qlik import sync

__SYNC_PACKAGE = 'google.datacatalog_connectors.qlik.sync'
_SYNCR_MODULE = f'{__SYNC_PACKAGE}.metadata_synchronizer'


@mock.patch(f'{_SYNCR_MODULE}.ingest.DataCatalogMetadataIngestor')
@mock.patch(f'{_SYNCR_MODULE}.cleanup.DataCatalogMetadataCleaner')
@mock.patch(f'{_SYNCR_MODULE}.prepare.EntryRelationshipMapper')
class MetadataSynchronizerTest(unittest.TestCase):

    @mock.patch(f'{_SYNCR_MODULE}.prepare.AssembledEntryFactory')
    @mock.patch(f'{_SYNCR_MODULE}.scrape.MetadataScraper')
    def setUp(self, mock_scraper, mock_assembled_entry_factory):
        self.__synchronizer = sync.MetadataSynchronizer(
            qlik_server_address='test-server',
            qlik_ad_domain='test-domain',
            qlik_username='test-username',
            qlik_password='test-password',
            datacatalog_project_id='test-project-id',
            datacatalog_location_id='test-location-id')

    def test_constructor_should_set_instance_attributes(
            self, mock_mapper, mock_cleaner, mock_ingestor):

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

    def test_run_no_metadata_should_clean_but_not_ingest_metadata(
            self, mock_mapper, mock_cleaner, mock_ingestor):

        scraper = self.__synchronizer.__dict__[
            '_MetadataSynchronizer__metadata_scraper']

        self.__synchronizer.run()

        scraper.scrape_all_custom_property_definitions.assert_called_once()
        scraper.scrape_all_streams.assert_called_once()

        cleaner = mock_cleaner.return_value
        cleaner.delete_obsolete_metadata.assert_called_once()

        ingestor = mock_ingestor.return_value
        ingestor.ingest_metadata.assert_not_called()

    def test_run_custom_property_def_should_traverse_main_workflow_steps(
            self, mock_mapper, mock_cleaner, mock_ingestor):

        attrs = self.__synchronizer.__dict__
        scraper = attrs['_MetadataSynchronizer__metadata_scraper']
        assembled_entry_factory = attrs[
            '_MetadataSynchronizer__assembled_entry_factory']

        scraper.scrape_all_custom_property_definitions.return_value = [{
            'id': 'test_def',
        }]
        assembled_entry_factory.make_assembled_entry_for_custom_property_def\
            .return_value = prepare.AssembledEntryData(
                'test_def',
                self.__make_fake_entry('custom_property_definition'),
                [])

        self.__synchronizer.run()

        expected_make_assembled_entries_call_arg = {
            'id': 'test_def',
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

    def test_run_stream_metadata_should_traverse_main_workflow_steps(
            self, mock_mapper, mock_cleaner, mock_ingestor):

        attrs = self.__synchronizer.__dict__
        scraper = attrs['_MetadataSynchronizer__metadata_scraper']
        assembled_entry_factory = attrs[
            '_MetadataSynchronizer__assembled_entry_factory']

        scraper.scrape_all_streams.return_value = [self.__make_fake_stream()]
        assembled_entry_factory.make_assembled_entries_for_stream\
            .return_value = [prepare.AssembledEntryData(
                'test_stream', self.__make_fake_entry('stream'), [])]

        self.__synchronizer.run()

        expected_make_assembled_entries_call_arg = {
            'id': 'test_stream',
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

    def test_run_stream_metadata_should_process_only_required_template(
            self, mock_mapper, mock_cleaner, mock_ingestor):

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
                'test_stream', fake_entry, [fake_tag])]

        self.__synchronizer.run()

        ingest_metadata_call_args = \
            mock_ingestor.return_value.ingest_metadata.call_args[0]
        templates_dict_call_arg = ingest_metadata_call_args[1]

        self.assertEqual(1, len(templates_dict_call_arg))
        self.assertTrue('qlik_stream_metadata' in templates_dict_call_arg)

    def test_run_published_app_should_properly_ask_assembled_entries(
            self, mock_mapper, mock_cleaner, mock_ingestor):

        attrs = self.__synchronizer.__dict__
        scraper = attrs['_MetadataSynchronizer__metadata_scraper']
        assembled_entry_factory = attrs[
            '_MetadataSynchronizer__assembled_entry_factory']

        scraper.scrape_all_streams.return_value = [self.__make_fake_stream()]
        scraper.scrape_all_apps.return_value = \
            [self.__make_fake_published_app()]
        scraper.scrape_sheets.return_value = []

        self.__synchronizer.run()

        expected_make_assembled_entries_call_arg = {
            'id':
                'test_stream',
            'apps': [{
                'id': 'test_app',
                'published': True,
                'stream': {
                    'id': 'test_stream'
                },
                'sheets': [],
            }]
        }

        actual_call_args = assembled_entry_factory\
            .make_assembled_entries_for_stream.call_args[0]
        self.assertEqual(expected_make_assembled_entries_call_arg,
                         actual_call_args[0])

    def test_run_not_published_app_should_properly_ask_assembled_entries(
            self, mock_mapper, mock_cleaner, mock_ingestor):

        attrs = self.__synchronizer.__dict__
        scraper = attrs['_MetadataSynchronizer__metadata_scraper']
        assembled_entry_factory = attrs[
            '_MetadataSynchronizer__assembled_entry_factory']

        scraper.scrape_all_streams.return_value = [self.__make_fake_stream()]
        scraper.scrape_all_apps.return_value = [self.__make_fake_wip_app()]
        assembled_entry_factory.make_assembled_entries_for_stream\
            .return_value = [
                prepare.AssembledEntryData(
                    'test_stream', self.__make_fake_entry('stream'), [])
            ]

        self.__synchronizer.run()

        expected_make_assembled_entries_call_arg = {
            'id': 'test_stream',
        }

        actual_call_args = assembled_entry_factory\
            .make_assembled_entries_for_stream.call_args[0]
        self.assertEqual(expected_make_assembled_entries_call_arg,
                         actual_call_args[0])

    def test_run_published_sheet_should_properly_ask_assembled_entries(
            self, mock_mapper, mock_cleaner, mock_ingestor):

        attrs = self.__synchronizer.__dict__
        scraper = attrs['_MetadataSynchronizer__metadata_scraper']
        assembled_entry_factory = attrs[
            '_MetadataSynchronizer__assembled_entry_factory']

        scraper.scrape_all_streams.return_value = [self.__make_fake_stream()]
        scraper.scrape_all_apps.return_value = \
            [self.__make_fake_published_app()]
        scraper.scrape_sheets.return_value = [
            self.__make_fake_published_sheet()
        ]

        self.__synchronizer.run()

        expected_make_assembled_entries_call_arg = {
            'id':
                'test_stream',
            'apps': [{
                'id':
                    'test_app',
                'published':
                    True,
                'stream': {
                    'id': 'test_stream'
                },
                'sheets': [{
                    'qInfo': {
                        'qId': 'test_sheet',
                    },
                    'qMeta': {
                        'published': True,
                    },
                    'app': {
                        'id': 'test_app',
                        'name': None
                    },
                }],
            }]
        }

        actual_call_args = assembled_entry_factory\
            .make_assembled_entries_for_stream.call_args[0]
        self.assertEqual(expected_make_assembled_entries_call_arg,
                         actual_call_args[0])

    def test_run_not_published_sheet_should_properly_ask_assembled_entries(
            self, mock_mapper, mock_cleaner, mock_ingestor):

        attrs = self.__synchronizer.__dict__
        scraper = attrs['_MetadataSynchronizer__metadata_scraper']
        assembled_entry_factory = attrs[
            '_MetadataSynchronizer__assembled_entry_factory']

        scraper.scrape_all_streams.return_value = [self.__make_fake_stream()]
        scraper.scrape_all_apps.return_value = \
            [self.__make_fake_published_app()]
        scraper.scrape_sheets.return_value = [self.__make_fake_wip_sheet()]

        self.__synchronizer.run()

        expected_make_assembled_entries_call_arg = {
            'id':
                'test_stream',
            'apps': [{
                'id': 'test_app',
                'published': True,
                'stream': {
                    'id': 'test_stream'
                },
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
            'id': 'test_stream',
        }

    @classmethod
    def __make_fake_published_app(cls):
        return {
            'id': 'test_app',
            'published': True,
            'stream': cls.__make_fake_stream(),
        }

    @classmethod
    def __make_fake_wip_app(cls):
        return {
            'id': 'test_app',
            'published': False,
        }

    @classmethod
    def __make_fake_sheet(cls):
        return {
            'qInfo': {
                'qId': 'test_sheet',
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
