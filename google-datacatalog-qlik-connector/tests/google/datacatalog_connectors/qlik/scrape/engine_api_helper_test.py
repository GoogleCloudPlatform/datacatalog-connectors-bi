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
    __HELPER_MODULE = f'{_SCRAPE_PACKAGE}.engine_api_helper'

    def setUp(self):
        # Set a constant seed so generated numbers will always be the same when
        # running the tests.
        random.seed(1)

        self.__helper = engine_api_helper.EngineAPIHelper(
            server_address='https://test-server',
            ad_domain='test-domain',
            username='test-username',
            password='test-password')

    def test_constructor_should_set_instance_attributes(self, mock_websocket):
        attrs = self.__helper.__dict__

        self.assertEqual('https://test-server',
                         attrs['_EngineAPIHelper__server_address'])
        self.assertEqual('test-domain', attrs['_EngineAPIHelper__ad_domain'])
        self.assertEqual('test-username', attrs['_EngineAPIHelper__username'])
        self.assertEqual('test-password', attrs['_EngineAPIHelper__password'])

        self.assertEqual('wss://test-server',
                         attrs['_EngineAPIHelper__base_api_endpoint'])
        self.assertIsNotNone(attrs['_EngineAPIHelper__common_headers'])

    def test_constructor_should_not_set_auth_related_attributes(
            self, mock_websocket):

        attrs = self.__helper.__dict__
        self.assertIsNone(attrs['_EngineAPIHelper__auth_cookie'])

    @mock.patch(f'{__HELPER_MODULE}.authenticator.Authenticator')
    def test_scrape_operations_should_authenticate_user_beforehand(
            self, mock_authenticator, mock_websocket):

        mock_websocket.return_value.__enter__.return_value.set_data([
            {
                'id': 2202,
                'params': {
                    'loginUri': 'redirect-url'
                }
            },
            {
                'id': 9326,
                'result': {
                    'qReturn': {}
                }
            },
        ])

        mock_authenticator.get_qps_session_cookie_windows_auth.return_value = \
            mock.MagicMock()

        # Call a public method to trigger the authentication workflow.
        self.__helper.get_sheets('app-id')

        attrs = self.__helper.__dict__
        extra_headers = attrs['_EngineAPIHelper__common_headers'].copy()
        extra_headers['User-Agent'] = 'Windows'

        expected_first_ws_call_args = mock.call(
            uri=f'wss://test-server/app/?transient=?Xrfkey={constants.XRFKEY}'
            f'&reloadUri=https://test-server/dev-hub/engine-api-explorer',
            extra_headers=extra_headers)
        actual_first_ws_call_args = mock_websocket.call_args_list[0]
        self.assertEqual(expected_first_ws_call_args,
                         actual_first_ws_call_args)

        mock_authenticator.get_qps_session_cookie_windows_auth\
            .assert_called_once()
        mock_authenticator.get_qps_session_cookie_windows_auth \
            .assert_called_with(
                ad_domain='test-domain',
                username='test-username',
                password='test-password',
                auth_url='redirect-url')
        self.assertIsNotNone(attrs['_EngineAPIHelper__auth_cookie'])

    def test_websocket_connection_should_use_cookie(self, mock_websocket):
        mock_websocket.return_value.__enter__.return_value.set_data([
            {
                'id': 2202,
                'result': {
                    'qReturn': {}
                }
            },
        ])

        attrs = self.__helper.__dict__
        attrs['_EngineAPIHelper__auth_cookie'] = \
            scrape_ops_mocks.FakeQPSSessionCookie()

        self.__helper.get_sheets('app-id')

        mock_websocket.assert_called_once()

        extra_headers = attrs['_EngineAPIHelper__common_headers'].copy()
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
                    'qList': [{
                        'qInfo': {
                            'qId': 'sheet-id',
                            'qType': 'sheet'
                        },
                    }]
                }
            },
        ])

        self.__helper.__dict__[
            '_EngineAPIHelper__auth_cookie'] = mock.MagicMock()

        sheets = self.__helper.get_sheets('app-id')

        self.assertEqual(1, len(sheets))
        self.assertEqual('sheet-id', sheets[0].get('qInfo').get('qId'))

    def test_get_sheets_should_return_none_on_no_server_response(
            self, mock_websocket):

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
                'id': 0,
                'result': {
                    'qList': [{
                        'qInfo': {
                            'qId': 'sheet-id',
                            'qType': 'sheet'
                        },
                    }]
                }
            },
        ])

        self.__helper.__dict__[
            '_EngineAPIHelper__auth_cookie'] = mock.MagicMock()

        sheets = self.__helper.get_sheets('app-id')

        self.assertIsNone(sheets)
