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

from google.datacatalog_connectors.tableau import scrape

from . import metadata_scraper_mocks


class MetadataScraperTest(unittest.TestCase):
    __SCRAPE_PACKAGE = 'google.datacatalog_connectors.tableau.scrape'
    __REST_API_HELPER_CLASS = f'{__SCRAPE_PACKAGE}.rest_api_helper' \
                              f'.RestAPIHelper'
    __METADATA_API_HELPER_CLASS = f'{__SCRAPE_PACKAGE}.metadata_api_helper' \
                                  f'.MetadataAPIHelper'

    @mock.patch(f'{__METADATA_API_HELPER_CLASS}.fetch_dashboards')
    @mock.patch(f'{__REST_API_HELPER_CLASS}.get_all_sites_for_server',
                metadata_scraper_mocks.mock_get_default_site)
    def test_scrape_dashboards_should_return_nonempty_list_on_success(
            self, mock_fetch_dashboards):

        mock_fetch_dashboards.return_value = [
            {
                'luid': 'TEST-ID-1',
            },
            {
                'luid': 'TEST-ID-2',
            },
        ]

        metadata = scrape.MetadataScraper(
            server_address='https://test-server.com',
            api_version='test-api',
            username='test-username',
            password='test-password').scrape_dashboards()

        self.assertEqual(2, len(metadata))

    @mock.patch(f'{__METADATA_API_HELPER_CLASS}.fetch_sites')
    @mock.patch(f'{__REST_API_HELPER_CLASS}.get_all_sites_for_server',
                metadata_scraper_mocks.mock_get_default_site)
    def test_scrape_sites_should_return_nonempty_list_on_success(
            self, mock_fetch_sites):

        mock_fetch_sites.return_value = [
            {
                'luid': 'TEST-ID-1',
            },
            {
                'luid': 'TEST-ID-2',
            },
        ]

        metadata = scrape.MetadataScraper(
            server_address='https://test-server.com',
            api_version='test-api',
            username='test-username',
            password='test-password').scrape_sites()

        self.assertEqual(2, len(metadata))

    @mock.patch(f'{__METADATA_API_HELPER_CLASS}.fetch_workbooks')
    @mock.patch(f'{__REST_API_HELPER_CLASS}.get_all_sites_for_server',
                metadata_scraper_mocks.mock_get_default_site)
    def test_scrape_workbooks_should_return_nonempty_list_on_success(
            self, mock_fetch_workbooks):

        mock_fetch_workbooks.return_value = [
            {
                'luid': 'TEST-ID-1',
            },
            {
                'luid': 'TEST-ID-2',
            },
        ]

        metadata = scrape.MetadataScraper(
            server_address='https://test-server.com',
            api_version='test-api',
            username='test-username',
            password='test-password').scrape_workbooks()

        self.assertEqual(2, len(metadata))

    @mock.patch(f'{__METADATA_API_HELPER_CLASS}.fetch_sites')
    @mock.patch(f'{__REST_API_HELPER_CLASS}.get_all_sites_for_server')
    def test_scrape_metadata_multiple_sites_should_fetch_assets_from_all(
            self, mock_get_all_sites_for_server, mock_fetch_sites):

        # The 'contentUrl' field is actually informed as an empty string for
        # the Default site and fulfilled for all other sites created by the
        # users.
        mock_get_all_sites_for_server.return_value = [
            {
                'id': 'TEST-ID-1',
                'name': 'Default',
                'contentUrl': '',
            },
            {
                'id': 'TEST-ID-2',
                'name': 'My site',
                'contentUrl': 'my-site',
            },
        ]

        scrape.MetadataScraper(server_address='https://test-server.com',
                               api_version='test-api',
                               username='test-username',
                               password='test-password').scrape_sites()

        mock_get_all_sites_for_server.assert_called_once()
        self.assertEqual(2, mock_fetch_sites.call_count)

    @mock.patch(f'{__METADATA_API_HELPER_CLASS}.fetch_sites')
    @mock.patch(f'{__REST_API_HELPER_CLASS}.get_all_sites_for_server')
    def test_scrape_metadata_specific_site_should_fetch_assets_given_site(
            self, mock_get_all_sites_for_server, mock_fetch_sites):

        scrape.MetadataScraper(
            server_address='https://test-server.com',
            api_version='test-api',
            username='test-username',
            password='test-password',
            site_content_url='test-site-url').scrape_sites()

        mock_get_all_sites_for_server.assert_not_called()
        mock_fetch_sites.assert_called_once()

    @mock.patch(f'{__METADATA_API_HELPER_CLASS}.fetch_sites')
    @mock.patch(f'{__REST_API_HELPER_CLASS}.get_all_sites_for_server')
    def test_scrape_metadata_no_sites_available_should_not_fetch_assets(
            self, mock_get_all_sites_for_server, mock_fetch_sites):

        mock_get_all_sites_for_server.return_value = []

        scrape.MetadataScraper(server_address='https://test-server.com',
                               api_version='test-api',
                               username='test-username',
                               password='test-password').scrape_sites()

        mock_get_all_sites_for_server.assert_called_once()
        mock_fetch_sites.assert_not_called()
