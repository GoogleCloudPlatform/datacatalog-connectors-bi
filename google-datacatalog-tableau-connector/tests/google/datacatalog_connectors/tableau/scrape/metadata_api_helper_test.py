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

import json
import unittest
from unittest import mock

from google.datacatalog_connectors.tableau.scrape import metadata_api_helper

from . import metadata_scraper_mocks


class MetadataAPIHelperTest(unittest.TestCase):
    __SCRAPE_PACKAGE = 'google.datacatalog_connectors.tableau.scrape'
    __HELPER_MODULE = f'{__SCRAPE_PACKAGE}.metadata_api_helper'

    def setUp(self):
        self.__helper = metadata_api_helper.MetadataAPIHelper(
            server_address='test-server',
            api_version='test-api',
            username='test-username',
            password='test-password',
            site_content_url='test-site-url')

    def test_constructor_should_set_instance_attributes(self):
        attrs = self.__helper.__dict__

        self.assertEqual('test-server',
                         attrs['_MetadataAPIHelper__server_address'])
        self.assertEqual('test-api', attrs['_MetadataAPIHelper__api_version'])
        self.assertEqual('test-username',
                         attrs['_MetadataAPIHelper__username'])
        self.assertEqual('test-password',
                         attrs['_MetadataAPIHelper__password'])
        self.assertEqual('test-site-url',
                         attrs['_MetadataAPIHelper__site_content_url'])

        self.assertEqual('test-server/relationship-service-war/graphql',
                         attrs['_MetadataAPIHelper__api_endpoint'])

    def test_constructor_should_not_set_auth_related_attributes(self):
        attrs = self.__helper.__dict__
        self.assertIsNone(attrs['_MetadataAPIHelper__auth_credentials'])

    @mock.patch(f'{__HELPER_MODULE}.requests', mock.MagicMock())
    @mock.patch(f'{__HELPER_MODULE}.authenticator.Authenticator.authenticate')
    def test_scrape_operations_should_authenticate_user_beforehand(
            self, mock_authenticate):

        mock_authenticate.return_value = {'token': 'TEST-TOKEN'}

        # Call a public method to trigger the authentication workflow.
        self.__helper.fetch_sites()

        mock_authenticate.assert_called_once()

        attrs = self.__helper.__dict__
        auth_credentials = attrs['_MetadataAPIHelper__auth_credentials']
        self.assertIsNotNone(auth_credentials)
        self.assertEqual({'token': 'TEST-TOKEN'}, auth_credentials)

    @mock.patch(f'{__HELPER_MODULE}.requests.post')
    def test_fetch_dashboards_should_return_nonempty_list_on_success(
            self, mock_post):

        attrs = self.__helper.__dict__
        attrs['_MetadataAPIHelper__auth_credentials'] = {'token': 'TEST-TOKEN'}

        mock_post.return_value = metadata_scraper_mocks.make_fake_response(
            {'data': {
                'dashboards': [{
                    'luid': 'TEST-ID-1',
                }]
            }}, 200)

        dashboards = self.__helper.fetch_dashboards()

        self.assertEqual(1, len(dashboards))
        self.assertEqual('TEST-ID-1', dashboards[0]['luid'])

    @mock.patch(f'{__HELPER_MODULE}.requests.post')
    def test_fetch_dashboards_should_add_site_content_url_return_value(
            self, mock_post):

        attrs = self.__helper.__dict__
        attrs['_MetadataAPIHelper__auth_credentials'] = {'token': 'TEST-TOKEN'}

        mock_post.return_value = metadata_scraper_mocks.make_fake_response(
            {
                'data': {
                    'dashboards': [{
                        'luid': 'TEST-ID-1',
                        'workbook': {
                            'site': {},
                        },
                    }]
                }
            }, 200)

        dashboards = self.__helper.fetch_dashboards()

        self.assertEqual('test-site-url',
                         dashboards[0]['workbook']['site']['contentUrl'])

    @mock.patch(f'{__HELPER_MODULE}.requests.post')
    def test_fetch_dashboards_should_return_empty_list_on_unexpected_response(
            self, mock_post):

        attrs = self.__helper.__dict__
        attrs['_MetadataAPIHelper__auth_credentials'] = {'token': 'TEST-TOKEN'}

        mock_post.return_value = metadata_scraper_mocks.make_fake_response(
            {'dashboards': [{
                'luid': 'TEST-ID-1',
            }]}, 200)

        dashboards = self.__helper.fetch_dashboards()

        self.assertEqual(0, len(dashboards))

    @mock.patch(f'{__HELPER_MODULE}.requests.post')
    def test_fetch_sites_should_return_nonempty_list_on_success(
            self, mock_post):

        attrs = self.__helper.__dict__
        attrs['_MetadataAPIHelper__auth_credentials'] = {'token': 'TEST-TOKEN'}

        mock_post.return_value = metadata_scraper_mocks.make_fake_response(
            {'data': {
                'tableauSites': [{
                    'luid': 'TEST-ID-1',
                }]
            }}, 200)

        sites = self.__helper.fetch_sites()

        self.assertEqual(1, len(sites))
        self.assertEqual('TEST-ID-1', sites[0]['luid'])

    @mock.patch(f'{__HELPER_MODULE}.requests.post')
    def test_fetch_sites_should_add_site_content_url_return_value(
            self, mock_post):

        attrs = self.__helper.__dict__
        attrs['_MetadataAPIHelper__auth_credentials'] = {'token': 'TEST-TOKEN'}

        mock_post.return_value = metadata_scraper_mocks.make_fake_response(
            {'data': {
                'tableauSites': [{
                    'workbooks': [{
                        'site': {}
                    }]
                }]
            }}, 200)

        sites = self.__helper.fetch_sites()

        self.assertEqual('test-site-url', sites[0]['contentUrl'])

    @mock.patch(f'{__HELPER_MODULE}.requests.post')
    def test_fetch_sites_should_return_empty_list_on_unexpected_response(
            self, mock_post):

        attrs = self.__helper.__dict__
        attrs['_MetadataAPIHelper__auth_credentials'] = {'token': 'TEST-TOKEN'}

        mock_post.return_value = metadata_scraper_mocks.make_fake_response(
            {'tableauSites': [{
                'luid': 'TEST-ID-1',
            }]}, 200)

        sites = self.__helper.fetch_sites()

        self.assertEqual(0, len(sites))

    @mock.patch(f'{__HELPER_MODULE}.requests.post')
    def test_fetch_workbooks_should_return_nonempty_list_on_success(
            self, mock_post):

        attrs = self.__helper.__dict__
        attrs['_MetadataAPIHelper__auth_credentials'] = {'token': 'TEST-TOKEN'}

        mock_post.return_value = metadata_scraper_mocks.make_fake_response(
            {'data': {
                'workbooks': [{
                    'luid': 'TEST-ID-1',
                }]
            }}, 200)

        workbooks = self.__helper.fetch_workbooks()

        self.assertEqual(1, len(workbooks))
        self.assertEqual('TEST-ID-1', workbooks[0]['luid'])

    @mock.patch(f'{__HELPER_MODULE}.requests.post')
    def test_fetch_workbooks_should_add_site_content_url_return_value(
            self, mock_post):

        attrs = self.__helper.__dict__
        attrs['_MetadataAPIHelper__auth_credentials'] = {'token': 'TEST-TOKEN'}

        mock_post.return_value = metadata_scraper_mocks.make_fake_response(
            {'data': {
                'workbooks': [{
                    'site': {}
                }]
            }}, 200)

        workbooks = self.__helper.fetch_workbooks()

        self.assertEqual('test-site-url', workbooks[0]['site']['contentUrl'])

    @mock.patch(f'{__HELPER_MODULE}.requests.post')
    def test_fetch_workbooks_should_return_empty_list_on_unexpected_response(
            self, mock_post):

        attrs = self.__helper.__dict__
        attrs['_MetadataAPIHelper__auth_credentials'] = {'token': 'TEST-TOKEN'}

        mock_post.return_value = metadata_scraper_mocks.make_fake_response(
            {'workbooks': [{
                'luid': 'TEST-ID-1',
            }]}, 200)

        workbooks = self.__helper.fetch_workbooks()

        self.assertEqual(0, len(workbooks))

    @mock.patch(f'{__HELPER_MODULE}.requests.post')
    def test_fetch_workbooks_using_filter_should_post_filter_variable(
            self, mock_post):

        attrs = self.__helper.__dict__
        attrs['_MetadataAPIHelper__auth_credentials'] = {'token': 'TEST-TOKEN'}

        self.__helper.fetch_workbooks(query_filter={'luid': '123456789'})

        args, kwargs = mock_post.call_args_list[0]
        variables = json.loads(kwargs['json']['variables'])
        self.assertEqual({'filter': {'luid': '123456789'}}, variables)
