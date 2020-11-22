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

from . import scrape_ops_mocks


class MetadataScraperTest(unittest.TestCase):
    __SCRAPE_PACKAGE = 'google.datacatalog_connectors.qlik.scrape'
    __SCRAPER_MODULE = f'{__SCRAPE_PACKAGE}.metadata_scraper'

    @mock.patch(f'{__SCRAPER_MODULE}.repository_services_api_helper'
                f'.RepositoryServicesAPIHelper')
    def setUp(self, mock_qrs_api_helper):
        self.__scraper = scrape.MetadataScraper(server_address='test-server',
                                                ad_domain='test-domain',
                                                username='test-username',
                                                password='test-password')

    def test_constructor_should_set_instance_attributes(self):
        attrs = self.__scraper.__dict__

        self.assertEqual('test-server',
                         attrs['_MetadataScraper__server_address'])
        self.assertEqual('test-domain', attrs['_MetadataScraper__ad_domain'])
        self.assertEqual('test-username', attrs['_MetadataScraper__username'])
        self.assertEqual('test-password', attrs['_MetadataScraper__password'])
        self.assertIsNotNone(attrs['_MetadataScraper__session'])
        self.assertIsNotNone(attrs['_MetadataScraper__qrs_api_helper'])

    @mock.patch(f'{__SCRAPER_MODULE}.authenticator.Authenticator')
    def test_scrape_should_authenticate_user(self, mock_authenticator):
        attrs = self.__scraper.__dict__
        qrs_api_helper = attrs['_MetadataScraper__qrs_api_helper']

        streams_metadata = [{
            'id': 'stream-id',
        }]

        qrs_api_helper.get_windows_authentication_url.return_value = 'test-url'
        mock_authenticator.get_qps_session_cookie_windows_auth.return_value =\
            scrape_ops_mocks.FakeQPSSessionCookie()
        qrs_api_helper.get_full_stream_list.return_value = streams_metadata

        streams = self.__scraper.scrape_streams()

        self.assertEqual(1, len(streams))
        self.assertEqual('stream-id', streams[0].get('id'))
        mock_authenticator.get_qps_session_cookie_windows_auth\
            .assert_called_with(
                ad_domain='test-domain',
                username='test-username',
                password='test-password',
                auth_url='test-url')
        qrs_api_helper.get_full_stream_list.assert_called_once()

    def test_scrape_streams_should_return_list_on_success(self):
        attrs = self.__scraper.__dict__
        qrs_api_helper = attrs['_MetadataScraper__qrs_api_helper']

        streams_metadata = [{
            'id': 'stream-id',
        }]

        attrs['_MetadataScraper__session'] = \
            scrape_ops_mocks.FakeSessionWithCookies()
        qrs_api_helper.get_full_stream_list.return_value = streams_metadata

        streams = self.__scraper.scrape_streams()

        self.assertEqual(1, len(streams))
        self.assertEqual('stream-id', streams[0].get('id'))
        qrs_api_helper.get_full_stream_list.assert_called_once()
