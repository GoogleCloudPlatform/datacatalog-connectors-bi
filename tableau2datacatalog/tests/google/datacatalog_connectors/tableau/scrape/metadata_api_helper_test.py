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

import json
import unittest
from unittest import mock

from google.datacatalog_connectors.tableau.scrape import metadata_api_helper

__SCRAPE_PACKAGE = 'google.datacatalog_connectors.tableau.scrape'


@mock.patch(f'{__SCRAPE_PACKAGE}.authenticator.requests.post')
class MetadataAPIHelperTest(unittest.TestCase):

    def setUp(self):
        self.__reader = metadata_api_helper.MetadataAPIHelper(
            server_address='https://test-server.com',
            auth_credentials={'token': 'TEST-TOKEN'})

    def test_fetch_workbooks_using_filter_should_post_filter_variable(
            self, mock_post):  # noqa: E125

        self.__reader.fetch_workbooks(query_filter={'luid': '123456789'})

        args, kwargs = mock_post.call_args_list[0]
        variables = json.loads(kwargs['json']['variables'])
        self.assertEqual({'filter': {'luid': '123456789'}}, variables)
