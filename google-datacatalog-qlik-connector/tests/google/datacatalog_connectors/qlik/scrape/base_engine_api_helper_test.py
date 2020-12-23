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
    base_engine_api_helper, constants

from . import scrape_ops_mocks

_SCRAPE_PACKAGE = 'google.datacatalog_connectors.qlik.scrape'


@mock.patch(f'{_SCRAPE_PACKAGE}.base_engine_api_helper.websockets.connect',
            new_callable=scrape_ops_mocks.AsyncContextManager)
class BaseEngineAPIHelperTest(unittest.TestCase):
    __SCRAPER_MODULE = f'{_SCRAPE_PACKAGE}.engine_api_scraper'

    def setUp(self):
        self.__scraper = base_engine_api_helper.BaseEngineAPIHelper(
            server_address='https://test-server', auth_cookie=mock.MagicMock())

    def test_constructor_should_set_instance_attributes(self, mock_websocket):
        attrs = self.__scraper.__dict__

        self.assertEqual('wss://test-server',
                         attrs['_BaseEngineAPIHelper__base_api_endpoint'])
        self.assertIsNotNone(attrs['_BaseEngineAPIHelper__auth_cookie'])
        self.assertIsNotNone(attrs['_BaseEngineAPIHelper__common_headers'])
        self.assertEqual(0, attrs['_BaseEngineAPIHelper__request_id'])

    def test_connect_websocket_should_use_cookie(self, mock_websocket):

        attrs = self.__scraper.__dict__
        attrs['_BaseEngineAPIHelper__auth_cookie'] = \
            scrape_ops_mocks.FakeQPSSessionCookie()

        self.__scraper._connect_websocket('app-id')

        mock_websocket.assert_called_once()

        extra_headers = attrs['_BaseEngineAPIHelper__common_headers'].copy()
        extra_headers['Cookie'] = 'X-Qlik-Session=Test cookie'
        mock_websocket.assert_called_with(
            uri=f'wss://test-server/app/app-id?Xrfkey={constants.XRFKEY}',
            extra_headers=extra_headers)
