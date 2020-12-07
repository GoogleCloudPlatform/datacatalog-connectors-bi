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

from google.datacatalog_connectors.tableau.scrape import rest_api_helper

from . import metadata_scraper_mocks


class RestAPIHelperTest(unittest.TestCase):
    __SCRAPE_PACKAGE = 'google.datacatalog_connectors.tableau.scrape'
    __HELPER_MODULE = f'{__SCRAPE_PACKAGE}.rest_api_helper'

    def setUp(self):
        self.__helper = rest_api_helper.RestAPIHelper(
            server_address='test-server',
            api_version='test-api',
            username='test-username',
            password='test-password',
            site_content_url='test-site-url')

    def test_constructor_should_set_instance_attributes(self):
        attrs = self.__helper.__dict__

        self.assertEqual('test-server',
                         attrs['_RestAPIHelper__server_address'])
        self.assertEqual('test-api', attrs['_RestAPIHelper__api_version'])
        self.assertEqual('test-username', attrs['_RestAPIHelper__username'])
        self.assertEqual('test-password', attrs['_RestAPIHelper__password'])
        self.assertEqual('test-site-url',
                         attrs['_RestAPIHelper__site_content_url'])

        self.assertEqual('test-server/api/test-api',
                         attrs['_RestAPIHelper__base_api_endpoint'])
        self.assertIsNotNone(attrs['_RestAPIHelper__common_headers'])

    def test_constructor_should_not_set_auth_related_attributes(self):
        attrs = self.__helper.__dict__
        self.assertIsNone(attrs['_RestAPIHelper__auth_credentials'])

    @mock.patch(f'{__HELPER_MODULE}.requests', mock.MagicMock())
    @mock.patch(f'{__HELPER_MODULE}.authenticator.Authenticator.authenticate')
    def test_scrape_operations_should_authenticate_user_beforehand(
            self, mock_authenticate):

        mock_authenticate.return_value = {'token': 'TEST-TOKEN'}

        # Call a public method to trigger the authentication workflow.
        self.__helper.get_all_sites_for_server()

        mock_authenticate.assert_called_once()

        attrs = self.__helper.__dict__
        auth_credentials = attrs['_RestAPIHelper__auth_credentials']
        self.assertIsNotNone(auth_credentials)
        self.assertEqual({'token': 'TEST-TOKEN'}, auth_credentials)

    @mock.patch(f'{__HELPER_MODULE}.requests.get')
    def test_get_all_sites_for_server_should_return_nonempty_list_on_success(
            self, mock_get):

        attrs = self.__helper.__dict__
        attrs['_RestAPIHelper__auth_credentials'] = {'token': 'TEST-TOKEN'}

        mock_get.return_value = metadata_scraper_mocks.make_fake_response(
            {'sites': {
                'site': [{
                    'luid': 'TEST-ID-1',
                }]
            }}, 200)

        sites = self.__helper.get_all_sites_for_server()

        self.assertEqual(1, len(sites))
        self.assertEqual('TEST-ID-1', sites[0]['luid'])

        headers = {
            'Content-Type': 'application/json',
            'Accept': 'application/json',
            'X-Tableau-Auth': 'TEST-TOKEN',
        }
        mock_get.assert_called_with(url='test-server/api/test-api/sites',
                                    headers=headers)

    @mock.patch(f'{__HELPER_MODULE}.requests.get')
    def test_get_all_sites_for_server_should_return_empty_list_on_unexpected_response(  # noqa E510
            self, mock_get):

        attrs = self.__helper.__dict__
        attrs['_RestAPIHelper__auth_credentials'] = {'token': 'TEST-TOKEN'}

        mock_get.return_value = metadata_scraper_mocks.make_fake_response(
            {'sites': [{
                'luid': 'TEST-ID-1',
            }]}, 200)

        sites = self.__helper.get_all_sites_for_server()

        self.assertEqual(0, len(sites))
