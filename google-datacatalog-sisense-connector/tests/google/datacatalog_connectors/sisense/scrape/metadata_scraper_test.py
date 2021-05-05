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

import unittest
from unittest import mock

from google.datacatalog_connectors.sisense import scrape


class MetadataScraperTest(unittest.TestCase):
    __SCRAPE_PACKAGE = 'google.datacatalog_connectors.sisense.scrape'
    __SCRAPER_MODULE = f'{__SCRAPE_PACKAGE}.metadata_scraper'

    @mock.patch(f'{__SCRAPER_MODULE}.rest_api_helper.RESTAPIHelper')
    def setUp(self, mock_rest_api_helper):
        self.__scraper = scrape.MetadataScraper(server_address='test-server',
                                                api_version='test-version',
                                                username='test-username',
                                                password='test-password')

    def test_constructor_should_set_instance_attributes(self):
        attrs = self.__scraper.__dict__
        self.assertIsNotNone(attrs['_MetadataScraper__api_helper'])

    def test_scrape_all_folders_should_delegate_to_api_helper(self):
        attrs = self.__scraper.__dict__
        api_helper = attrs['_MetadataScraper__api_helper']

        folders_metadata = [{
            '_id': 'folder-id',
        }]

        api_helper.get_all_folders.return_value = folders_metadata

        folders = self.__scraper.scrape_all_folders()

        self.assertEqual(1, len(folders))
        self.assertEqual('folder-id', folders[0]['_id'])
        api_helper.get_all_folders.assert_called_once()
