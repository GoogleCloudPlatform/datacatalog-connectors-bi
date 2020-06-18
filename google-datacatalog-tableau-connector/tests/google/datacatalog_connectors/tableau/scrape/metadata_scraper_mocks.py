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


class __MockResponse:

    def __init__(self, json_data, status_code):
        self.__json_data = json_data
        self.__status_code = status_code

    def json(self):
        return self.__json_data


def make_fake_response(json_data, status_code):
    return __MockResponse(json_data, status_code)


def mock_authenticate(server_address,
                      api_version,
                      username,
                      password,
                      site=None):
    return {'token': 'TEST-TOKEN'}


def mock_get_default_site(self):
    return [{'contentUrl': ''}]


def mock_empty_response(url, data=None, json=None, **kwargs):
    return make_fake_response({}, 200)
