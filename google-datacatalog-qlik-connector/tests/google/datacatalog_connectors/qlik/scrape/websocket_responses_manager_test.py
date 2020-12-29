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

from google.datacatalog_connectors.qlik.scrape import \
    websocket_responses_manager


class WebsocketResponsesManagerTest(unittest.TestCase):

    def setUp(self):
        self.__manager = \
            websocket_responses_manager.WebsocketResponsesManager()

    def test_init_pending_ids_holder_should_ignore_empty_holder_key(self):
        self.__manager.init_pending_ids_holder('')

        attrs = self.__manager.__dict__
        self.assertEqual(
            0, len(attrs['_WebsocketResponsesManager__pending_response_ids']))

    def test_init_pending_ids_holders_should_ignore_empty_holder_keys(self):
        self.__manager.init_pending_ids_holders([])

        attrs = self.__manager.__dict__
        self.assertEqual(
            0, len(attrs['_WebsocketResponsesManager__pending_response_ids']))
