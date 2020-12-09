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

from google.datacatalog_connectors.qlik.scrape import \
    constants, repository_services_api_helper

from . import scrape_ops_mocks


class RepositoryServicesAPIHelperTest(unittest.TestCase):
    __SCRAPE_PACKAGE = 'google.datacatalog_connectors.qlik.scrape'
    __HELPER_MODULE = f'{__SCRAPE_PACKAGE}.repository_services_api_helper'

    def setUp(self):
        self.__helper = \
            repository_services_api_helper.RepositoryServicesAPIHelper(
                server_address='test-server',
                ad_domain='test-domain',
                username='test-username',
                password='test-password')

    def test_constructor_should_set_instance_attributes(self):
        attrs = self.__helper.__dict__

        self.assertEqual('test-server',
                         attrs['_RepositoryServicesAPIHelper__server_address'])
        self.assertEqual('test-domain',
                         attrs['_RepositoryServicesAPIHelper__ad_domain'])
        self.assertEqual('test-username',
                         attrs['_RepositoryServicesAPIHelper__username'])
        self.assertEqual('test-password',
                         attrs['_RepositoryServicesAPIHelper__password'])

        self.assertEqual(
            'test-server/qrs',
            attrs['_RepositoryServicesAPIHelper__base_api_endpoint'])
        self.assertIsNotNone(
            attrs['_RepositoryServicesAPIHelper__common_headers'])

    def test_constructor_should_not_set_session_related_attributes(self):
        attrs = self.__helper.__dict__
        self.assertIsNone(attrs['_RepositoryServicesAPIHelper__http_session'])

    @mock.patch(f'{__HELPER_MODULE}.sessions.Session.get')
    @mock.patch(f'{__HELPER_MODULE}.requests')
    @mock.patch(f'{__HELPER_MODULE}.authenticator.Authenticator')
    def test_scrape_operations_should_authenticate_user_beforehand(
            self, mock_authenticator, mock_requests, mock_session_get):

        fake_cookie = scrape_ops_mocks.FakeQPSSessionCookie()

        mock_requests.get.return_value = \
            scrape_ops_mocks.FakeResponseWithHeader(
                'location', 'redirect-url', True)
        mock_authenticator.get_qps_session_cookie_windows_auth.return_value = \
            fake_cookie
        mock_session_get.return_value = \
            scrape_ops_mocks.FakeResponseWithContent('[]')

        # Call a public method to trigger the authentication workflow.
        self.__helper.get_full_stream_list()

        attrs = self.__helper.__dict__
        headers = attrs['_RepositoryServicesAPIHelper__common_headers'].copy()
        headers['User-Agent'] = 'Windows'
        mock_requests.get.assert_called_with(
            url=f'test-server/qrs/about?Xrfkey={constants.XRFKEY}',
            headers=headers,
            allow_redirects=False)

        mock_authenticator.get_qps_session_cookie_windows_auth \
            .assert_called_once()
        mock_authenticator.get_qps_session_cookie_windows_auth \
            .assert_called_with(
                ad_domain='test-domain',
                username='test-username',
                password='test-password',
                auth_url='redirect-url')

        http_session = attrs['_RepositoryServicesAPIHelper__http_session']
        self.assertIsNotNone(http_session)
        self.assertEqual('Test cookie',
                         http_session.cookies.get('X-Qlik-Session'))

    @mock.patch(f'{__HELPER_MODULE}.sessions.Session')
    def test_get_full_app_list_should_return_list_on_success(
            self, mock_session):

        mock_session.get.return_value = \
            scrape_ops_mocks.FakeResponseWithContent('[{\"id\": \"app-id\"}]')

        attrs = self.__helper.__dict__
        attrs['_RepositoryServicesAPIHelper__http_session'] = mock_session

        apps = self.__helper.get_full_app_list()

        self.assertEqual(1, len(apps))
        self.assertEqual('app-id', apps[0].get('id'))
        mock_session.get.assert_called_once()
        mock_session.get.assert_called_with(
            url=f'test-server/qrs/app/hublist/full?Xrfkey={constants.XRFKEY}',
            headers=attrs['_RepositoryServicesAPIHelper__common_headers'],
        )

    @mock.patch(f'{__HELPER_MODULE}.sessions.Session')
    def test_get_full_stream_list_should_return_list_on_success(
            self, mock_session):

        mock_session.get.return_value = \
            scrape_ops_mocks.FakeResponseWithContent(
                '[{\"id\": \"stream-id\"}]')

        attrs = self.__helper.__dict__
        attrs['_RepositoryServicesAPIHelper__http_session'] = mock_session

        streams = self.__helper.get_full_stream_list()

        self.assertEqual(1, len(streams))
        self.assertEqual('stream-id', streams[0].get('id'))
        mock_session.get.assert_called_once()
        mock_session.get.assert_called_with(
            url=f'test-server/qrs/stream/full?Xrfkey={constants.XRFKEY}',
            headers=attrs['_RepositoryServicesAPIHelper__common_headers'],
        )
