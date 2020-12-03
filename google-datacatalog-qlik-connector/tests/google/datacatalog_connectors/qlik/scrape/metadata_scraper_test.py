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

from google.datacatalog_connectors.qlik import scrape


class MetadataScraperTest(unittest.TestCase):
    __SCRAPE_PACKAGE = 'google.datacatalog_connectors.qlik.scrape'
    __SCRAPER_MODULE = f'{__SCRAPE_PACKAGE}.metadata_scraper'

    @mock.patch(f'{__SCRAPER_MODULE}.engine_api_helper.EngineAPIHelper')
    @mock.patch(f'{__SCRAPER_MODULE}.repository_services_api_helper'
                f'.RepositoryServicesAPIHelper')
    def setUp(self, mock_qrs_api_helper, mock_engine_api_helper):
        self.__scraper = scrape.MetadataScraper(server_address='test-server',
                                                ad_domain='test-domain',
                                                username='test-username',
                                                password='test-password')

    def test_constructor_should_set_instance_attributes(self):
        attrs = self.__scraper.__dict__
        self.assertIsNotNone(attrs['_MetadataScraper__qrs_api_helper'])
        self.assertIsNotNone(attrs['_MetadataScraper__engine_api_helper'])

    def test_scrape_all_apps_should_return_list_on_success(self):
        attrs = self.__scraper.__dict__
        qrs_api_helper = attrs['_MetadataScraper__qrs_api_helper']

        apps_metadata = [{
            'id': 'app-id',
        }]

        attrs['_MetadataScraper__qrs_api_session'] = mock.MagicMock()
        qrs_api_helper.get_full_app_list.return_value = apps_metadata

        apps = self.__scraper.scrape_all_apps()

        self.assertEqual(1, len(apps))
        self.assertEqual('app-id', apps[0].get('id'))
        qrs_api_helper.get_full_app_list.assert_called_once()

    def test_scrape_all_streams_should_return_list_on_success(self):
        attrs = self.__scraper.__dict__
        qrs_api_helper = attrs['_MetadataScraper__qrs_api_helper']

        streams_metadata = [{
            'id': 'stream-id',
        }]

        attrs['_MetadataScraper__qrs_api_session'] = mock.MagicMock()
        qrs_api_helper.get_full_stream_list.return_value = streams_metadata

        streams = self.__scraper.scrape_all_streams()

        self.assertEqual(1, len(streams))
        self.assertEqual('stream-id', streams[0].get('id'))
        qrs_api_helper.get_full_stream_list.assert_called_once()

    def test_scrape_sheets_should_return_list_on_success(self):
        attrs = self.__scraper.__dict__
        engine_api_helper = attrs['_MetadataScraper__engine_api_helper']

        sheets_metadata = [
            {
                'qInfo': {
                    'qId': 'sheet-id',
                    'qType': 'sheet',
                },
                'qMeta': {},
            },
        ]

        attrs['_MetadataScraper__engine_api_auth_cookie'] = mock.MagicMock()
        engine_api_helper.get_sheets.return_value = sheets_metadata

        sheets = self.__scraper.scrape_sheets({'id': 'app-id'})

        self.assertEqual(1, len(sheets))
        self.assertEqual('sheet-id', sheets[0].get('qInfo').get('qId'))
        engine_api_helper.get_sheets.assert_called_once()

    def test_scrape_sheets_should_return_empty_list_on_no_server_response(
            self):

        attrs = self.__scraper.__dict__
        engine_api_helper = attrs['_MetadataScraper__engine_api_helper']

        attrs['_MetadataScraper__engine_api_auth_cookie'] = mock.MagicMock()
        engine_api_helper.get_sheets.return_value = None

        sheets = self.__scraper.scrape_sheets({'id': 'app-id'})

        self.assertEqual(0, len(sheets))
        engine_api_helper.get_sheets.assert_called_once()
