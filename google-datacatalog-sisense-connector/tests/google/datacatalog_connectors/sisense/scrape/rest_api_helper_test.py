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

    @mock.patch(f'{__PRIVATE_METHOD_PREFIX}__get_response_body_or_raise')
    @mock.patch(f'{__HELPER_MODULE}.requests', mock.MagicMock())
    @mock.patch(f'{__PRIVATE_METHOD_PREFIX}__set_up_auth', lambda *args: None)
    def test_get_all_dashboards_should_return_list_on_success(
            self, mock_get_response_body_or_raise):

        mock_get_response_body_or_raise.return_value = [{
            '_id': 'dashboard-id'
        }]

        dashboards = self.__helper.get_all_dashboards()

        self.assertEqual(1, len(dashboards))
        self.assertEqual('dashboard-id', dashboards[0]['_id'])

    @mock.patch(f'{__HELPER_CLASS}.get_dashboard')
    @mock.patch(f'{__PRIVATE_METHOD_PREFIX}__get_response_body_or_raise')
    @mock.patch(f'{__HELPER_MODULE}.requests', mock.MagicMock())
    @mock.patch(f'{__PRIVATE_METHOD_PREFIX}__set_up_auth', lambda *args: None)
    def test_get_all_dashboards_should_get_detail_of_shared_dashboards(
            self, mock_get_response_body_or_raise, mock_get_dashboard):

        mock_get_response_body_or_raise.return_value = [{
            'oid': 'dashboard-oid'
        }]

        mock_get_dashboard.return_value = {'_id': 'dashboard-id'}

        dashboards = self.__helper.get_all_dashboards()

        self.assertEqual(1, len(dashboards))
        self.assertEqual('dashboard-id', dashboards[0]['_id'])
        mock_get_dashboard.assert_called_once_with('dashboard-oid')

    @mock.patch(f'{__PRIVATE_METHOD_PREFIX}__get_response_body_or_raise')
    @mock.patch(f'{__HELPER_MODULE}.requests', mock.MagicMock())
    @mock.patch(f'{__PRIVATE_METHOD_PREFIX}__set_up_auth', lambda *args: None)
    def test_get_dashboard_should_return_object_on_success(
            self, mock_get_response_body_or_raise):

        mock_get_response_body_or_raise.return_value = {
            'oid': 'dashboard-id',
        }

        dashboard = self.__helper.get_dashboard('dashboard-id')

        self.assertEqual('dashboard-id', dashboard['oid'])

    @mock.patch(f'{__PRIVATE_METHOD_PREFIX}__get_list_using_pagination')
    @mock.patch(f'{__PRIVATE_METHOD_PREFIX}__set_up_auth', lambda *args: None)
    def test_get_all_folders_should_return_list_on_success(
            self, mock_get_list_using_pagination):

        mock_get_list_using_pagination.return_value = [{'oid': 'folder-id'}]

        folders = self.__helper.get_all_folders()

        self.assertEqual(1, len(folders))
        self.assertEqual('folder-id', folders[0]['oid'])

    @mock.patch(f'{__PRIVATE_METHOD_PREFIX}__get_list_using_pagination')
    @mock.patch(f'{__PRIVATE_METHOD_PREFIX}__set_up_auth', lambda *args: None)
    def test_get_all_folders_should_use_pagination(
            self, mock_get_list_using_pagination):

        self.__helper.get_all_folders()

        mock_get_list_using_pagination.assert_called_once_with(
            base_url='test-server/api/test-version/folders?structure=flat',
            results_per_page=50,
        )

    @mock.patch(f'{__PRIVATE_METHOD_PREFIX}__get_response_body_or_raise')
    @mock.patch(f'{__HELPER_MODULE}.requests', mock.MagicMock())
    @mock.patch(f'{__PRIVATE_METHOD_PREFIX}__set_up_auth', lambda *args: None)
    def test_get_user_should_return_object_on_success(
            self, mock_get_response_body_or_raise):

        mock_get_response_body_or_raise.return_value = {
            'oid': 'user-id',
        }

        user = self.__helper.get_user('user-id')

        self.assertEqual('user-id', user['oid'])

    @mock.patch(f'{__PRIVATE_METHOD_PREFIX}__get_list_using_pagination')
    @mock.patch(f'{__PRIVATE_METHOD_PREFIX}__set_up_auth', lambda *args: None)
    def test_get_widgets_should_return_list_on_success(
            self, mock_get_list_using_pagination):

        mock_get_list_using_pagination.return_value = [{'oid': 'widget-id'}]

        widgets = self.__helper.get_widgets('dashboard-id')

        self.assertEqual(1, len(widgets))
        self.assertEqual('widget-id', widgets[0]['oid'])

    @mock.patch(f'{__PRIVATE_METHOD_PREFIX}__get_list_using_pagination')
    @mock.patch(f'{__PRIVATE_METHOD_PREFIX}__set_up_auth', lambda *args: None)
    def test_get_widgets_should_use_pagination(self,
                                               mock_get_list_using_pagination):

        self.__helper.get_widgets('dashboard-id')

        mock_get_list_using_pagination.assert_called_once_with(
            base_url='test-server/api/test-version'
            '/dashboards/dashboard-id/widgets',
            results_per_page=50,
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

    def test_get_list_using_pagination_should_validate_results_per_page(self):
        self.assertRaises(
            ValueError,
            self.__helper._RESTAPIHelper__get_list_using_pagination, '', 1)

    @mock.patch(f'{__PRIVATE_METHOD_PREFIX}__get_response_body_or_raise')
    @mock.patch(f'{__HELPER_MODULE}.requests')
    def test_get_list_using_pagination_should_get_all_pages(
            self, mock_requests, mock_get_response_body_or_raise):

        mock_get_response_body_or_raise.side_effect = [[{
            'oid': 'object-id-1'
        }, {
            'oid': 'object-id-2'
        }], [{
            'oid': 'object-id-3'
        }, {
            'oid': 'object-id-4'
        }], [{
            'oid': 'object-id-5'
        }]]

        results = self.__helper._RESTAPIHelper__get_list_using_pagination(
            'test-url', 2)

        self.assertEqual(5, len(results))
        self.assertEqual(3, mock_get_response_body_or_raise.call_count)

        attrs = self.__helper.__dict__
        first_call = mock.call(
            url='test-url?skip=0&limit=2',
            headers=attrs['_RESTAPIHelper__common_headers'],
        )
        second_call = mock.call(
            url='test-url?skip=2&limit=2',
            headers=attrs['_RESTAPIHelper__common_headers'],
        )
        third_call = mock.call(
            url='test-url?skip=4&limit=2',
            headers=attrs['_RESTAPIHelper__common_headers'],
        )
        mock_requests.get.has_calls([first_call, second_call, third_call])

    @mock.patch(f'{__PRIVATE_METHOD_PREFIX}__get_response_body_or_raise')
    @mock.patch(f'{__HELPER_MODULE}.requests', mock.MagicMock())
    def test_get_list_using_pagination_should_remove_duplicates(
            self, mock_get_response_body_or_raise):

        mock_get_response_body_or_raise.side_effect = [[{
            'oid': 'object-id-1'
        }, {
            'oid': 'object-id-2'
        }], [{
            'oid': 'object-id-2'
        }]]

        results = self.__helper._RESTAPIHelper__get_list_using_pagination(
            'test-url', 2)

        self.assertEqual(2, len(results))
        self.assertEqual(2, mock_get_response_body_or_raise.call_count)

    def test_get_response_body_or_raise_should_get_on_success(self):
        body = rest_api_helper.RESTAPIHelper\
            ._RESTAPIHelper__get_response_body_or_raise(
                metadata_scraper_mocks.FakeResponse({'oid': 'object-id'}))

        self.assertIsNotNone(body)
        self.assertEqual('object-id', body['oid'])

    def test_get_response_body_or_raise_should_raise_on_api_error(self):
        self.assertRaises(
            Exception, rest_api_helper.RESTAPIHelper.
            _RESTAPIHelper__get_response_body_or_raise,
            metadata_scraper_mocks.FakeResponse(
                {
                    'error': {
                        'code': 101,
                        'message': 'Access denied',
                        'status': 403,
                        'httpMessage': 'Forbidden'
                    }
                }, 403))
