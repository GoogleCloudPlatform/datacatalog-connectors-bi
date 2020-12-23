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

from google.datacatalog_connectors.qlik.scrape import engine_api_sheets_helper

from . import scrape_ops_mocks

_SCRAPE_PACKAGE = 'google.datacatalog_connectors.qlik.scrape'


@mock.patch(f'{_SCRAPE_PACKAGE}.base_engine_api_helper'
            f'.BaseEngineAPIHelper._connect_websocket',
            new_callable=scrape_ops_mocks.AsyncContextManager)
class EngineAPISheetsHelperTest(unittest.TestCase):

    def setUp(self):
        self.__helper = engine_api_sheets_helper.EngineAPISheetsHelper(
            server_address='https://test-server', auth_cookie=mock.MagicMock())

    def test_constructor_should_set_instance_attributes(self, mock_websocket):
        attrs = self.__helper.__dict__
        self.assertIsNotNone(attrs['_EngineAPISheetsHelper__responses_queue'])

    def test_get_sheets_should_return_list_on_success(self, mock_websocket):
        websocket_ctx = mock_websocket.return_value.__enter__.return_value
        websocket_ctx.set_itr_break(0.25)
        websocket_ctx.set_data([
            {
                'id': 1,
                'result': {
                    'qReturn': {
                        'qHandle': 1,
                    },
                },
            },
            {
                'id': 2,
                'result': {
                    'qList': [{
                        'qInfo': {
                            'qId': 'sheet-id',
                            'qType': 'sheet',
                        },
                    }],
                },
            },
        ])

        sheets = self.__helper.get_sheets('app-id')

        self.assertEqual(1, len(sheets))
        self.assertEqual('sheet-id', sheets[0].get('qInfo').get('qId'))

    # TODO Create a mechanism to exit the stream after waiting for the message
    #  for a longer time.
    """
    def test_get_sheets_should_return_empty_list_on_no_suitable_response(
            self, mock_websocket):

        websocket_ctx = mock_websocket.return_value.__enter__.return_value
        websocket_ctx.set_itr_break(0.25)
        websocket_ctx.set_data([
            {
                'id': 1,
                'result': {
                    'qReturn': {
                        'qHandle': 1,
                    },
                },
            },
            {
                'id': 5,  # The expected id is 2. 5 means an unknown request.
                'result': {
                    'qList': [{
                        'qInfo': {
                            'qId': 'sheet-id',
                            'qType': 'sheet',
                        },
                    }],
                },
            },
        ])

        sheets = self.__helper.get_sheets('app-id')

        self.assertEqual(0, len(sheets))
    """
