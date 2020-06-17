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

from .metadata_scraper_mocks import make_fake_response, \
    mock_authenticate, mock_get_default_site, mock_empty_response


class MetadataScraperTest(unittest.TestCase):
    __SCRAPE_PACKAGE = 'google.datacatalog_connectors.tableau.scrape'

    @mock.patch(f'{__SCRAPE_PACKAGE}.authenticator.requests.post')
    @mock.patch(f'{__SCRAPE_PACKAGE}.metadata_api_helper'
                f'.MetadataAPIHelper.fetch_workbooks', lambda self, *args: [])
    def test_scrape_metadata_should_authenticate_user(self,
                                                      mock_post_authenticate):

        mock_post_authenticate.return_value = make_fake_response(
            {'credentials': {
                'token': 'TEST-TOKEN'
            }}, 200)

        scrape.MetadataScraper(server_address=None,
                               api_version=None,
                               username=None,
                               password=None,
                               site='test').scrape_workbooks()

        self.assertEqual(mock_post_authenticate.call_count, 1)

    @mock.patch(f'{__SCRAPE_PACKAGE}.authenticator.Authenticator.authenticate',
                mock_authenticate)
    @mock.patch(f'{__SCRAPE_PACKAGE}.rest_api_helper.requests.get')
    def test_scrape_metadata_no_sites_available_should_not_raise_exception(
            self, mock_get_sites):  # noqa: E125

        mock_get_sites.return_value = make_fake_response({}, 200)

        scrape.MetadataScraper(server_address=None,
                               api_version=None,
                               username=None,
                               password=None).scrape_workbooks()

        self.assertEqual(mock_get_sites.call_count, 1)

    @mock.patch(f'{__SCRAPE_PACKAGE}.authenticator.Authenticator.authenticate',
                mock_authenticate)
    @mock.patch(f'{__SCRAPE_PACKAGE}.rest_api_helper.RestAPIHelper'
                f'.get_all_sites_on_server', mock_get_default_site)
    @mock.patch(f'{__SCRAPE_PACKAGE}.metadata_api_helper.requests.post')
    def test_scrape_dashboards_should_return_nonempty_list(
            self, mock_post_query_dashboards):  # noqa: E125

        mock_post_query_dashboards.return_value = make_fake_response(
            {
                'data': {
                    'dashboards': [{
                        'luid': 'TEST-ID-1'
                    }, {
                        'luid': 'TEST-ID-2'
                    }]
                }
            }, 200)

        metadata = scrape.MetadataScraper(server_address=None,
                                          api_version=None,
                                          username=None,
                                          password=None).scrape_dashboards()

        self.assertEqual(2, len(metadata))

    @mock.patch(f'{__SCRAPE_PACKAGE}.authenticator.Authenticator.authenticate',
                mock_authenticate)
    @mock.patch(f'{__SCRAPE_PACKAGE}.rest_api_helper.RestAPIHelper'
                f'.get_all_sites_on_server', mock_get_default_site)
    @mock.patch(f'{__SCRAPE_PACKAGE}.metadata_api_helper.requests.post',
                mock_empty_response)
    def test_scrape_dashboards_no_data_available_should_return_empty_list(
            self):  # noqa: E125

        metadata = scrape.MetadataScraper(server_address=None,
                                          api_version=None,
                                          username=None,
                                          password=None).scrape_dashboards()

        self.assertEqual(0, len(metadata))

    @mock.patch(f'{__SCRAPE_PACKAGE}.authenticator.Authenticator.authenticate',
                mock_authenticate)
    @mock.patch(f'{__SCRAPE_PACKAGE}.rest_api_helper.RestAPIHelper'
                f'.get_all_sites_on_server', mock_get_default_site)
    @mock.patch(f'{__SCRAPE_PACKAGE}.metadata_api_helper.requests.post')
    def test_scrape_sites_should_return_nonempty_list(
            self, mock_post_query_sites):  # noqa: E125

        mock_post_query_sites.return_value = make_fake_response(
            {
                'data': {
                    'tableauSites': [{
                        'luid': 'TEST-ID-1'
                    }, {
                        'luid': 'TEST-ID-2'
                    }]
                }
            }, 200)

        metadata = scrape.MetadataScraper(server_address=None,
                                          api_version=None,
                                          username=None,
                                          password=None).scrape_sites()

        self.assertEqual(2, len(metadata))

    @mock.patch(f'{__SCRAPE_PACKAGE}.authenticator.Authenticator.authenticate',
                mock_authenticate)
    @mock.patch(f'{__SCRAPE_PACKAGE}.rest_api_helper.RestAPIHelper'
                f'.get_all_sites_on_server', mock_get_default_site)
    @mock.patch(f'{__SCRAPE_PACKAGE}.metadata_api_helper.requests.post',
                mock_empty_response)
    def test_scrape_sites_no_data_available_should_return_empty_list(self):
        metadata = scrape.MetadataScraper(server_address=None,
                                          api_version=None,
                                          username=None,
                                          password=None).scrape_sites()

        self.assertEqual(0, len(metadata))

    @mock.patch(f'{__SCRAPE_PACKAGE}.authenticator.Authenticator.authenticate',
                mock_authenticate)
    @mock.patch(f'{__SCRAPE_PACKAGE}.rest_api_helper.RestAPIHelper'
                f'.get_all_sites_on_server', mock_get_default_site)
    @mock.patch(f'{__SCRAPE_PACKAGE}.metadata_api_helper.requests.post')
    def test_scrape_workbooks_should_return_nonempty_list(
            self, mock_post_query_workbooks):  # noqa: E125

        mock_post_query_workbooks.return_value = make_fake_response(
            {
                'data': {
                    'workbooks': [{
                        'luid': 'TEST-ID-1'
                    }, {
                        'luid': 'TEST-ID-2'
                    }]
                }
            }, 200)

        metadata = scrape.MetadataScraper(server_address=None,
                                          api_version=None,
                                          username=None,
                                          password=None).scrape_workbooks()

        self.assertEqual(2, len(metadata))

    @mock.patch(f'{__SCRAPE_PACKAGE}.authenticator.Authenticator.authenticate',
                mock_authenticate)
    @mock.patch(f'{__SCRAPE_PACKAGE}.rest_api_helper.RestAPIHelper'
                f'.get_all_sites_on_server', mock_get_default_site)
    @mock.patch(f'{__SCRAPE_PACKAGE}.metadata_api_helper.requests.post',
                mock_empty_response)
    def test_scrape_workbooks_no_data_available_should_return_empty_list(self):
        metadata = scrape.MetadataScraper(server_address=None,
                                          api_version=None,
                                          username=None,
                                          password=None).scrape_workbooks()

        self.assertEqual(0, len(metadata))
