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
    communication sessions.

    Attributes:
        __pending_ids:
            A ``list`` containing the ids of the pending responses, which means
            the requests identified by them were sent but not answered yet.
        __api_calls_history:
            A ``dict`` containing a full history of the API calls known by a
            given ``WebsocketResponsesManager`` instance, represented as
            ``reponse-id: method`` items. It is automatically fulfilled when
            pending ids are added to ``__pending_ids``.
        __unhandled_responses:
            A ``list`` containing all response objects that were received but
            not handled yet.
        __new_response_event:
            A signal used by the consumer to notify the producer on the arrival
            of new responses, so the producer can take actions such as sending
            follow up requests.
        __interface_handles:
            A ``dict`` containing the keys and values of the interface handles
            required by a given communication session.
    """

    def __init__(self):
        self.__pending_ids = []
        self.__api_calls_history = {}
        self.__unhandled_responses = []
        self.__new_response_event = asyncio.Event()
        self.__interface_handles = {}

    def add_pending_id(self, response_id, method):
        self.__pending_ids.append(response_id)
        self.__api_calls_history[response_id] = method

    def add_pending_ids(self, response_ids, method):
        for response_id in response_ids:
            self.add_pending_id(response_id, method)

    def remove_pending_id(self, response_id):
        self.__pending_ids.remove(response_id)

    def is_pending(self, response_id, method):
        return response_id in self.__pending_ids and self.is_method(
            response_id, method)

    def is_method(self, response_id, method):
        return method == self.__api_calls_history.get(response_id)

    def add_unhandled(self, response):
        self.__unhandled_responses.append(response)

    def remove_unhandled(self, response):
        self.__unhandled_responses.remove(response)

    def get_all_unhandled(self):
        return self.__unhandled_responses

    def were_all_precessed(self):
        return not self.__pending_ids and not self.__unhandled_responses

    def notify_new_response(self):
        self.__new_response_event.set()

    async def wait_for_responses(self):
        return await self.__new_response_event.wait()

    def is_there_response_notification(self):
        return self.__new_response_event.is_set()

    def clear_response_notifications(self):
        self.__new_response_event.clear()

    def set_handle(self, value, handle_key):
        self.__interface_handles[handle_key] = value

    def get_handle(self, handle_key):
        return self.__interface_handles[handle_key]
