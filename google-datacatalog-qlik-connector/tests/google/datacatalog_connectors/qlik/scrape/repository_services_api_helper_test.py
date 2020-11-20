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

from requests import sessions
import unittest
from unittest import mock

from google.datacatalog_connectors.qlik.scrape import \
    constants, repository_services_api_helper


class RepositoryServicesAPIHelperTest(unittest.TestCase):
    __SCRAPE_PACKAGE = 'google.datacatalog_connectors.qlik.scrape'

    def setUp(self):
        self.__helper = \
            repository_services_api_helper.RepositoryServicesAPIHelper(
                server_address='test-server')

    def test_constructor_should_set_instance_attributes(self):
        attrs = self.__helper.__dict__

        self.assertEqual(
            'test-server/qrs',
            attrs['_RepositoryServicesAPIHelper__base_api_endpoint'])
        self.assertIsNotNone(attrs['_RepositoryServicesAPIHelper__headers'])

    @mock.patch(f'{__SCRAPE_PACKAGE}.repository_services_api_helper.requests')
    def test_get_windows_authentication_url_should_return_url_from_header(
            self, mock_requests):
        mock_session = mock.Mock(sessions.Session())
        mock_session.get_redirect_target.return_value = 'redirect-url'

        url = self.__helper.get_windows_authentication_url(mock_session)

        self.assertEqual('redirect-url', url)
        mock_requests.get.assert_called_once()

    def test_get_streams_should_use_session_to_call_api(self):
        mock_session = mock.Mock(sessions.Session())

        streams = self.__helper.get_streams(mock_session)

        self.assertIsNotNone(streams)
        mock_session.get.assert_called_once()
        mock_session.get.assert_called_with(
            url=f'test-server/qrs/stream/full?Xrfkey={constants.XRFKEY}',
            headers=self.__helper.
            __dict__['_RepositoryServicesAPIHelper__headers'],
        )
