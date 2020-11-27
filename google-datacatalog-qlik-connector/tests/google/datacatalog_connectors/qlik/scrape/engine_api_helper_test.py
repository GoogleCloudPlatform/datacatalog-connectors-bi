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

import random
import unittest
from unittest import mock

from google.datacatalog_connectors.qlik.scrape import \
    constants, engine_api_helper

from . import scrape_ops_mocks

_SCRAPE_PACKAGE = 'google.datacatalog_connectors.qlik.scrape'


@mock.patch(f'{_SCRAPE_PACKAGE}.engine_api_helper.websockets.connect',
            new_callable=scrape_ops_mocks.AsyncContextManager)
class EngineAPIHelperTest(unittest.TestCase):

    def setUp(self):
        # Set a constant seed to ensure generated numbers will always be the
        # same when running the tests.
        random.seed(1)

        self.__helper = engine_api_helper.EngineAPIHelper(
            server_address='https://test-server')

    def test_constructor_should_set_instance_attributes(self, mock_websocket):
        attrs = self.__helper.__dict__

        self.assertEqual('https://test-server',
                         attrs['_EngineAPIHelper__server_address'])
        self.assertEqual('wss://test-server',
                         attrs['_EngineAPIHelper__base_api_endpoint'])
        self.assertIsNotNone(attrs['_EngineAPIHelper__common_headers'])

    def test_websocket_connection_should_use_cookie(self, mock_websocket):
        mock_websocket.return_value.__enter__.return_value.set_data([
            {
                'id': 2202,
                'result': {
                    'qReturn': {}
                }
            },
        ])

        fake_cookie = scrape_ops_mocks.FakeQPSSessionCookie()
        self.__helper.get_sheets('app-id', fake_cookie)

        mock_websocket.assert_called_once()

        extra_headers = self.__helper.__dict__[
            '_EngineAPIHelper__common_headers'].copy()
        extra_headers['Cookie'] = 'X-Qlik-Session=Test cookie'
        mock_websocket.assert_called_with(
            uri=f'wss://test-server/app/app-id?Xrfkey={constants.XRFKEY}',
            extra_headers=extra_headers)

    def test_get_sheets_should_return_list_on_success(self, mock_websocket):
        mock_websocket.return_value.__enter__.return_value.set_data([
            {
                'id': 2202,
                'result': {
                    'qReturn': {
                        'qHandle': 1
                    }
                }
            },
            {
                'id': 9326,
                'result': {
                    'qList': []
                }
            },
        ])

        sheets = self.__helper.get_sheets({'id': 'app-id'}, mock.MagicMock())

        self.assertIsNotNone(sheets)

    def test_get_windows_authentication_url_should_return_url_from_response(
            self, mock_websocket):

        mock_websocket.return_value.__enter__.return_value.set_data([
            {
                'id': 2202,
                'params': {
                    'loginUri': 'redirect-url'
                }
            },
        ])

        url = self.__helper.get_windows_authentication_url()

        extra_headers = self.__helper.__dict__[
            '_EngineAPIHelper__common_headers'].copy()
        extra_headers['User-Agent'] = 'Windows'

        self.assertEqual('redirect-url', url)
        mock_websocket.assert_called_with(
            uri=f'wss://test-server/app/?transient=?Xrfkey={constants.XRFKEY}'
            f'&reloadUri=https://test-server/dev-hub/engine-api-explorer',
            extra_headers=extra_headers)
