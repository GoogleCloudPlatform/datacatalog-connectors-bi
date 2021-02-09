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

import asyncio
import unittest
from unittest import mock

from google.datacatalog_connectors.qlik.scrape import \
    engine_api_visualizations_helper

from . import scrape_ops_mocks


class EngineAPIVisualizationsHelperTest(unittest.TestCase):
    __SCRAPE_PACKAGE = 'google.datacatalog_connectors.qlik.scrape'
    __BASE_CLASS = f'{__SCRAPE_PACKAGE}.base_engine_api_helper' \
                   f'.BaseEngineAPIHelper'
    __HELPER_CLASS = f'{__SCRAPE_PACKAGE}.engine_api_visualizations_helper' \
                     f'.EngineAPIVisualizationsHelper'

    def setUp(self):
        self.__helper = \
            engine_api_visualizations_helper.EngineAPIVisualizationsHelper(
                server_address='https://test-server',
                auth_cookie=mock.MagicMock())

    @mock.patch(
        f'{__HELPER_CLASS}._EngineAPIVisualizationsHelper__get_visualizations',
        lambda *args: None)
    @mock.patch(f'{__BASE_CLASS}._run_until_complete')
    def test_get_visualizations_should_raise_unknown_exception(
            self, mock_run_until_complete):

        mock_run_until_complete.side_effect = Exception
        self.assertRaises(Exception, self.__helper.get_visualizations,
                          'app_id')

    @mock.patch(
        f'{__HELPER_CLASS}._EngineAPIVisualizationsHelper__get_visualizations',
        lambda *args: None)
    @mock.patch(f'{__BASE_CLASS}._run_until_complete')
    def test_get_visualizations_should_return_empty_list_on_timeout(
            self, mock_run_until_complete):

        mock_run_until_complete.side_effect = asyncio.TimeoutError
        visualizations = self.__helper.get_visualizations('app-id')
        self.assertEqual(0, len(visualizations))

    # BaseEngineAPIHelper._hold_websocket_communication is purposefully not
    # mocked in this test case in order to simulate a full send/reply scenario
    # with replies representing an App with Visualizations. Maybe it's worth
    # refactoring it in the future to mock that method, and the private async
    # ones from EngineAPIVisualizationsHelper as well, thus testing in a more
    # granular way.
    @mock.patch(f'{__BASE_CLASS}._generate_message_id')
    @mock.patch(f'{__BASE_CLASS}._send_get_all_infos_request')
    @mock.patch(f'{__BASE_CLASS}._BaseEngineAPIHelper__send_open_doc_message')
    @mock.patch(f'{__BASE_CLASS}._connect_websocket',
                new_callable=scrape_ops_mocks.AsyncContextManager)
    def test_get_visualizations_should_return_list_on_success(
            self, mock_websocket, mock_send_open_doc, mock_send_get_all_infos,
            mock_generate_message_id):

        mock_send_open_doc.return_value = asyncio.sleep(delay=0, result=1)
        mock_send_get_all_infos.return_value = asyncio.sleep(delay=0, result=2)
        mock_generate_message_id.side_effect = [3, 4]

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
                    'qInfos': [{
                        'qId': 'visualization-id',
                        'qType': 'masterobject'
                    }],
                },
            },
            {
                'id': 3,
                'result': {
                    'qReturn': {
                        'qHandle': 2,
                    },
                },
            },
            {
                'id': 4,
                'result': {
                    'qProp': [{
                        'qInfo': {
                            'qId': 'visualization-id',
                        },
                    }],
                },
            },
        ])

        visualizations = self.__helper.get_visualizations('app-id')

        self.assertEqual(1, len(visualizations))
        self.assertEqual('visualization-id',
                         visualizations[0].get('qInfo').get('qId'))
        mock_send_open_doc.assert_called_once()
        mock_send_get_all_infos.assert_called_once()

    # BaseEngineAPIHelper._hold_websocket_communication is purposefully not
    # mocked in this test case in order to simulate a full send/reply scenario
    # with replies representing an App with no Visualizations. Maybe it's worth
    # refactoring it in the future to mock that method, and the private async
    # ones from EngineAPIVisualizationsHelper as well, thus testing in a more
    # granular way.
    @mock.patch(f'{__BASE_CLASS}._send_get_all_infos_request')
    @mock.patch(f'{__BASE_CLASS}._BaseEngineAPIHelper__send_open_doc_message')
    @mock.patch(f'{__BASE_CLASS}._connect_websocket',
                new_callable=scrape_ops_mocks.AsyncContextManager)
    def test_get_visualizations_should_return_empty_list_on_none_available(
            self, mock_websocket, mock_send_open_doc, mock_send_get_all_infos):

        mock_send_open_doc.return_value = asyncio.sleep(delay=0, result=1)
        mock_send_get_all_infos.return_value = asyncio.sleep(delay=0, result=2)

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
                    'qInfos': [],
                },
            },
        ])

        visualizations = self.__helper.get_visualizations('app-id')

        self.assertEqual(0, len(visualizations))
        mock_send_open_doc.assert_called_once()
        mock_send_get_all_infos.assert_called_once()
