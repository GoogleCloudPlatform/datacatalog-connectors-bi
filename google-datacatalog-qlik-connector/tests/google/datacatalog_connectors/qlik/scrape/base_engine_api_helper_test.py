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

import asyncio
import unittest
from unittest import mock

from google.datacatalog_connectors.qlik.scrape import \
    base_engine_api_helper, constants

from . import scrape_ops_mocks


class BaseEngineAPIHelperTest(unittest.TestCase):
    __SCRAPE_PACKAGE = 'google.datacatalog_connectors.qlik.scrape'
    __HELPER_MODULE = f'{__SCRAPE_PACKAGE}.base_engine_api_helper'
    __HELPER_CLASS = f'{__HELPER_MODULE}.BaseEngineAPIHelper'

    def setUp(self):
        self.__helper = base_engine_api_helper.BaseEngineAPIHelper(
            server_address='https://test-server', auth_cookie=mock.MagicMock())

    def test_constructor_should_set_instance_attributes(self):
        attrs = self.__helper.__dict__

        self.assertEqual('wss://test-server',
                         attrs['_BaseEngineAPIHelper__base_api_endpoint'])
        self.assertIsNotNone(attrs['_BaseEngineAPIHelper__auth_cookie'])
        self.assertIsNotNone(attrs['_BaseEngineAPIHelper__common_headers'])
        self.assertEqual(0, attrs['_BaseEngineAPIHelper__requests_counter'])

    @mock.patch(f'{__HELPER_MODULE}.websockets.connect',
                new_callable=scrape_ops_mocks.AsyncContextManager)
    def test_connect_websocket_should_use_cookie(self, mock_websocket):

        attrs = self.__helper.__dict__
        attrs['_BaseEngineAPIHelper__auth_cookie'] = \
            scrape_ops_mocks.FakeQPSSessionCookie()

        self.__helper._connect_websocket('app-id')

        mock_websocket.assert_called_once()

        extra_headers = attrs['_BaseEngineAPIHelper__common_headers'].copy()
        extra_headers['Cookie'] = 'X-Qlik-Session=Test cookie'
        mock_websocket.assert_called_with(
            uri=f'wss://test-server/app/app-id?Xrfkey={constants.XRFKEY}',
            extra_headers=extra_headers)

    def test_generate_request_id_should_increment_on_each_call(self):
        self.assertEqual(1, self.__helper._generate_request_id())
        self.assertEqual(2, self.__helper._generate_request_id())
        self.assertEqual(3, self.__helper._generate_request_id())

    @mock.patch(f'{__HELPER_CLASS}._handle_event_loop_exec_timeout')
    def test_run_until_complete_should_stop_on_timeout(self,
                                                       mock_handle_timeout):

        self.assertRaises(
            asyncio.TimeoutError,
            base_engine_api_helper.BaseEngineAPIHelper._run_until_complete,
            asyncio.wait_for(asyncio.sleep(0.5), timeout=0.25))

        mock_handle_timeout.assert_called_once()

    @classmethod
    def test_handle_event_loop_exec_timeout_should_stop_loop(cls):
        mock_event_loop = mock.MagicMock()
        base_engine_api_helper.BaseEngineAPIHelper\
            ._handle_event_loop_exec_timeout(mock_event_loop)
        mock_event_loop.stop.assert_called_once()

    @mock.patch(f'{__HELPER_CLASS}._generate_request_id')
    @mock.patch(f'{__HELPER_MODULE}.websockets.connect',
                new_callable=scrape_ops_mocks.AsyncContextManager)
    def test_start_websocket_communication_should_add_doc_interface_request_id(
            self, mock_websocket, mock_generate_request_id):

        mock_generate_request_id.return_value = 10
        mock_responses_manager = mock.MagicMock()

        base_engine_api_helper.BaseEngineAPIHelper._run_until_complete(
            self.__helper._start_websocket_communication(
                mock_websocket, 'app-id', mock_responses_manager))

        mock_generate_request_id.assert_called_once()
        mock_responses_manager.add_pending_id.assert_called_once_with(
            10, 'OpenDoc')

    @mock.patch(f'{__HELPER_CLASS}._generate_request_id')
    @mock.patch(f'{__HELPER_MODULE}.websockets.connect',
                new_callable=scrape_ops_mocks.AsyncContextManager)
    def test_send_get_all_infos_request_should_return_request_id(
            self, mock_websocket, mock_generate_request_id):

        mock_generate_request_id.return_value = 10
        request_id = base_engine_api_helper.BaseEngineAPIHelper\
            ._run_until_complete(
                self.__helper._send_get_all_infos_request(mock_websocket, 1))

        mock_generate_request_id.assert_called_once()
        self.assertEqual(10, request_id)

    @mock.patch(f'{__HELPER_CLASS}._handle_error_api_response')
    def test_handle_generic_api_response_should_handle_errors(
            self, mock_handle_error):

        base_engine_api_helper.BaseEngineAPIHelper\
            ._handle_generic_api_response({})
        mock_handle_error.assert_called_once()

    def test_handle_error_api_response_should_raise_on_max_parallel_sessions(
            self):

        self.assertRaises(
            Exception, base_engine_api_helper.BaseEngineAPIHelper.
            _handle_error_api_response, {
                'method': 'OnMaxParallelSessionsExceeded',
                'params': {},
            })
