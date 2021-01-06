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

from google.datacatalog_connectors.qlik import scrape


class MetadataScraperTest(unittest.TestCase):
    __SCRAPE_PACKAGE = 'google.datacatalog_connectors.qlik.scrape'
    __SCRAPER_MODULE = f'{__SCRAPE_PACKAGE}.metadata_scraper'

    @mock.patch(f'{__SCRAPER_MODULE}.engine_api_scraper.EngineAPIScraper')
    @mock.patch(f'{__SCRAPER_MODULE}.repository_services_api_helper'
                f'.RepositoryServicesAPIHelper')
    def setUp(self, mock_qrs_api_helper, mock_engine_api_scraper):
        self.__scraper = scrape.MetadataScraper(server_address='test-server',
                                                ad_domain='test-domain',
                                                username='test-username',
                                                password='test-password')

    def test_constructor_should_set_instance_attributes(self):
        attrs = self.__scraper.__dict__
        self.assertIsNotNone(attrs['_MetadataScraper__qrs_api_helper'])
        self.assertIsNotNone(attrs['_MetadataScraper__engine_api_scraper'])

    def test_scrape_all_apps_should_return_list_on_success(self):
        attrs = self.__scraper.__dict__
        qrs_api_helper = attrs['_MetadataScraper__qrs_api_helper']

        apps_metadata = [{
            'id': 'app-id',
        }]

        qrs_api_helper.get_full_app_list.return_value = apps_metadata

        apps = self.__scraper.scrape_all_apps()

        self.assertEqual(1, len(apps))
        self.assertEqual('app-id', apps[0].get('id'))
        qrs_api_helper.get_full_app_list.assert_called_once()

    def test_scrape_all_custom_property_definitions_should_return_list_on_success(  # noqa E510
            self):

        attrs = self.__scraper.__dict__
        qrs_api_helper = attrs['_MetadataScraper__qrs_api_helper']

        custom_property_defs_metadata = [{
            'id': 'custom-property-definition-id',
        }]

        qrs_api_helper.get_full_custom_property_definition_list\
            .return_value = custom_property_defs_metadata

        custom_property_defs = \
            self.__scraper.scrape_all_custom_property_definitions()

        self.assertEqual(1, len(custom_property_defs))
        self.assertEqual('custom-property-definition-id',
                         custom_property_defs[0].get('id'))
        qrs_api_helper.get_full_custom_property_definition_list\
            .assert_called_once()

    def test_scrape_all_streams_should_return_list_on_success(self):
        attrs = self.__scraper.__dict__
        qrs_api_helper = attrs['_MetadataScraper__qrs_api_helper']

        streams_metadata = [{
            'id': 'stream-id',
        }]

        qrs_api_helper.get_full_stream_list.return_value = streams_metadata

        streams = self.__scraper.scrape_all_streams()

        self.assertEqual(1, len(streams))
        self.assertEqual('stream-id', streams[0].get('id'))
        qrs_api_helper.get_full_stream_list.assert_called_once()

    def test_scrape_dimensions_should_return_list_on_success(self):
        attrs = self.__scraper.__dict__
        engine_api_scraper = attrs['_MetadataScraper__engine_api_scraper']

        dimensions_metadata = [
            {
                'qInfo': {
                    'qId': 'dimension-id',
                },
                'qMetaDef': {},
            },
        ]

        engine_api_scraper.get_dimensions.return_value = dimensions_metadata

        dimensions = self.__scraper.scrape_dimensions({'id': 'app-id'})

        self.assertEqual(1, len(dimensions))
        self.assertEqual('dimension-id', dimensions[0].get('qInfo').get('qId'))
        engine_api_scraper.get_dimensions.assert_called_once()

    def test_scrape_dimensions_should_return_empty_list_on_no_server_response(
            self):

        attrs = self.__scraper.__dict__
        engine_api_scraper = attrs['_MetadataScraper__engine_api_scraper']

        engine_api_scraper.get_dimensions.return_value = None

        dimensions = self.__scraper.scrape_dimensions({'id': 'app-id'})

        self.assertEqual(0, len(dimensions))
        engine_api_scraper.get_dimensions.assert_called_once()

    def test_scrape_measures_should_return_list_on_success(self):
        attrs = self.__scraper.__dict__
        engine_api_scraper = attrs['_MetadataScraper__engine_api_scraper']

        measures_metadata = [
            {
                'qInfo': {
                    'qId': 'measure-id',
                },
                'qMetaDef': {},
            },
        ]

        engine_api_scraper.get_measures.return_value = measures_metadata

        measures = self.__scraper.scrape_measures({'id': 'app-id'})

        self.assertEqual(1, len(measures))
        self.assertEqual('measure-id', measures[0].get('qInfo').get('qId'))
        engine_api_scraper.get_measures.assert_called_once()

    def test_scrape_measures_should_return_empty_list_on_no_server_response(
            self):

        attrs = self.__scraper.__dict__
        engine_api_scraper = attrs['_MetadataScraper__engine_api_scraper']

        engine_api_scraper.get_measures.return_value = None

        measures = self.__scraper.scrape_measures({'id': 'app-id'})

        self.assertEqual(0, len(measures))
        engine_api_scraper.get_measures.assert_called_once()

    def test_scrape_sheets_should_return_list_on_success(self):
        attrs = self.__scraper.__dict__
        engine_api_scraper = attrs['_MetadataScraper__engine_api_scraper']

        sheets_metadata = [
            {
                'qInfo': {
                    'qId': 'sheet-id',
                },
                'qMeta': {},
            },
        ]

        engine_api_scraper.get_sheets.return_value = sheets_metadata

        sheets = self.__scraper.scrape_sheets({'id': 'app-id'})

        self.assertEqual(1, len(sheets))
        self.assertEqual('sheet-id', sheets[0].get('qInfo').get('qId'))
        engine_api_scraper.get_sheets.assert_called_once()

    def test_scrape_sheets_should_return_empty_list_on_no_server_response(
            self):

        attrs = self.__scraper.__dict__
        engine_api_scraper = attrs['_MetadataScraper__engine_api_scraper']

        engine_api_scraper.get_sheets.return_value = None

        sheets = self.__scraper.scrape_sheets({'id': 'app-id'})

        self.assertEqual(0, len(sheets))
        engine_api_scraper.get_sheets.assert_called_once()

    def test_scrape_visualizations_should_return_list_on_success(self):
        attrs = self.__scraper.__dict__
        engine_api_scraper = attrs['_MetadataScraper__engine_api_scraper']

        visualizations_metadata = [
            {
                'qInfo': {
                    'qId': 'visualization-id',
                },
                'qMetaDef': {},
            },
        ]

        engine_api_scraper.get_visualizations.return_value = \
            visualizations_metadata

        visualizations = self.__scraper.scrape_visualizations({'id': 'app-id'})

        self.assertEqual(1, len(visualizations))
        self.assertEqual('visualization-id',
                         visualizations[0].get('qInfo').get('qId'))
        engine_api_scraper.get_visualizations.assert_called_once()

    def test_scrape_visualizations_should_return_empty_list_on_no_server_response(  # noqa E510
            self):

        attrs = self.__scraper.__dict__
        engine_api_scraper = attrs['_MetadataScraper__engine_api_scraper']

        engine_api_scraper.get_visualizations.return_value = None

        visualizations = self.__scraper.scrape_visualizations({'id': 'app-id'})

        self.assertEqual(0, len(visualizations))
        engine_api_scraper.get_visualizations.assert_called_once()
