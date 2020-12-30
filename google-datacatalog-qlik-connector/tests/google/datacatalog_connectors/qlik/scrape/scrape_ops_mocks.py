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
import json
from unittest import mock


# ======= #
# Cookies #
# ======= #
class FakeQPSSessionCookie:

    def __init__(self):
        self.name = 'X-Qlik-Session'
        self.value = 'Test cookie'
        self.domain = 'localhost'
        self.path = '/'


# ============== #
# HTTP Responses #
# ============== #
class FakeResponseWithContent:

    def __init__(self, content):
        self.__content = content

    def json(self):
        return json.loads(self.__content)


class FakeResponseWithCookies:

    def __init__(self):
        self.cookies = [FakeQPSSessionCookie()]


class FakeResponseWithNoCookies:

    def __init__(self):
        self.cookies = []


class FakeResponseWithHeader:

    def __init__(self, header_name, header_value, is_redirect=False):
        self.headers = {header_name: header_value}
        self.is_redirect = is_redirect


# =========================================================================== #
# Async code                                                                  #
#                                                                             #
# The classes belonging to this section were created to enable running unit   #
# tests for async code in Python < 3.8, which provides native support through #
# the 'AsyncMock' class. The present solution is based on the 'Strategies for #
# Testing Async Code in Python' blog post (https://www.agari.com/email-security-blog/strategies-testing-async-code-python/)  # noqa E510
# =========================================================================== #
class AsyncContextManager(mock.MagicMock):

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.__data = []
        self.__itr_index = -1
        self.__itr_break = 0

    async def __aenter__(self, *args, **kwargs):
        return self.__enter__(*args, **kwargs)

    async def __aexit__(self, *args, **kwargs):
        return self.__exit__(*args, **kwargs)

    def __aiter__(self):
        return self

    async def __anext__(self):
        await asyncio.sleep(self.__itr_break)
        self.__itr_index = self.__itr_index + 1
        if self.__itr_index >= len(self.__data):
            raise StopAsyncIteration
        return self.__data[self.__itr_index]

    def set_data(self, data):
        self.__data = [json.dumps(element) for element in data]
        self.__itr_index = -1

    def set_itr_break(self, itr_break):
        self.__itr_break = itr_break

    async def send(self, *args, **kwargs):
        pass

    async def close(self, *args, **kwargs):
        pass
