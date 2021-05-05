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

from google.datacatalog_connectors.sisense.scrape import rest_api_helper

from . import metadata_scraper_mocks


class RESTAPIHelperTest(unittest.TestCase):
    __SCRAPE_PACKAGE = 'google.datacatalog_connectors.sisense.scrape'
    __HELPER_MODULE = f'{__SCRAPE_PACKAGE}.rest_api_helper'
    __HELPER_CLASS = f'{__HELPER_MODULE}.RESTAPIHelper'
    __PRIVATE_METHOD_PREFIX = f'{__HELPER_CLASS}._RESTAPIHelper'

    def setUp(self):
        self.__helper = rest_api_helper.RESTAPIHelper(
            server_address='test-server',
            api_version='test-version',
            username='test-username',
            password='test-password')

    def test_constructor_should_set_instance_attributes(self):
        attrs = self.__helper.__dict__

        self.assertEqual('test-server',
                         attrs['_RESTAPIHelper__server_address'])
        self.assertEqual('test-version', attrs['_RESTAPIHelper__api_version'])
        self.assertEqual('test-username', attrs['_RESTAPIHelper__username'])
        self.assertEqual('test-password', attrs['_RESTAPIHelper__password'])

        self.assertEqual('test-server/api/test-version',
                         attrs['_RESTAPIHelper__base_api_endpoint'])
        self.assertIsNotNone(attrs['_RESTAPIHelper__common_headers'])

    def test_constructor_should_not_set_auth_related_attributes(self):
        attrs = self.__helper.__dict__
        self.assertIsNone(attrs['_RESTAPIHelper__auth_credentials'])

    @mock.patch(f'{__HELPER_MODULE}.requests')
    @mock.patch(f'{__PRIVATE_METHOD_PREFIX}__set_up_auth', lambda *args: None)
    def test_get_all_folders_should_return_list_on_success(
            self, mock_requests):

        mock_requests.get.return_value = metadata_scraper_mocks.FakeResponse([{
            '_id': 'folder-id'
        }])

        folders = self.__helper.get_all_folders()

        self.assertEqual(1, len(folders))
        self.assertEqual('folder-id', folders[0]['_id'])

        attrs = self.__helper.__dict__
        mock_requests.get.assert_called_once_with(
            url='test-server/api/test-version/folders?structure=flat',
            headers=attrs['_RESTAPIHelper__common_headers'],
        )

    @mock.patch(f'{__HELPER_MODULE}.authenticator.Authenticator.authenticate')
    def test_set_up_auth_should_set_credentials_if_not_authenticated(
            self, mock_authenticate):
        mock_authenticate.return_value = {'access_token': 'test-token'}

        self.__helper._RESTAPIHelper__set_up_auth()

        attrs = self.__helper.__dict__
        self.assertEqual({'access_token': 'test-token'},
                         attrs['_RESTAPIHelper__auth_credentials'])
        self.assertEqual(
            'Bearer test-token',
            attrs['_RESTAPIHelper__common_headers']['Authorization'])

    @mock.patch(f'{__HELPER_MODULE}.authenticator.Authenticator.authenticate')
    def test_set_up_auth_should_skip_if_already_authenticated(
            self, mock_authenticate):

        attrs = self.__helper.__dict__
        attrs['_RESTAPIHelper__auth_credentials'] = {
            'access_token': 'test-token'
        }

        self.__helper._RESTAPIHelper__set_up_auth()

        mock_authenticate.assert_not_called()
