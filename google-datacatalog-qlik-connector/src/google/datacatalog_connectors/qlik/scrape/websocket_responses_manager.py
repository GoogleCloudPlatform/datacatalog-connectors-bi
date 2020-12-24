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


class WebsocketResponsesManager:
    """Utility class with common features for responses handling over websocket
    communication sessions."""

    def __init__(self):
        self.__pending_response_ids = {}
        self.__unhandled_responses = []
        # Used by the consumer to notify the producer on new responses, so the
        # producer can take actions such as sending follow up requests.
        self.__new_response_event = asyncio.Event()

    def add_pending_ids_list(self, key):
        if not key:
            return

        self.__pending_response_ids[key] = []

    def add_pending_ids_lists(self, keys):
        if not keys:
            return

        for key in keys:
            self.add_pending_ids_list(key)

    def add_pending_id(self, response_id, list_key):
        self.__pending_response_ids[list_key].append(response_id)

    def remove_pending_id(self, response_id):
        for pending_ids in self.__pending_response_ids.values():
            if response_id in pending_ids:
                pending_ids.remove(response_id)

    def is_pending(self, response_id, list_key):
        return response_id in self.__pending_response_ids[list_key]

    def were_all_received(self):
        pending_count = sum([
            len(pending_ids)
            for pending_ids in self.__pending_response_ids.values()
        ])
        return pending_count == 0

    def clear_pending_ids(self):
        for pending_ids in self.__pending_response_ids.values():
            pending_ids.clear()

    def add_unhandled(self, response):
        self.__unhandled_responses.append(response)

    def remove_unhandled(self, response):
        self.__unhandled_responses.remove(response)

    def get_all_unhandled(self):
        return self.__unhandled_responses

    def notify_new_response(self):
        self.__new_response_event.set()

    async def wait_for_responses(self):
        return await self.__new_response_event.wait()

    def is_there_response_notification(self):
        return self.__new_response_event.is_set()

    def clear_response_notifications(self):
        self.__new_response_event.clear()
