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

from looker_sdk import error, models
from looker_sdk.rtl import serialize

from google.datacatalog_connectors.looker import scrape


class MetadataScraperTest(unittest.TestCase):

    @mock.patch('google.datacatalog_connectors.looker.scrape'
                '.metadata_scraper.client.setup')
    def setUp(self, mock_client):
        self.__scraper = scrape.MetadataScraper('looker-credentials-file.ini')

    def test_constructor_should_set_instance_attributes(self):
        self.assertIsNotNone(self.__scraper.__dict__['_MetadataScraper__sdk'])

    def test_scrape_dashboard_should_return_object_on_success(self):
        sdk = self.__scraper.__dict__['_MetadataScraper__sdk']

        dashboard_data = {
            'id': 'dashboard-id',
        }

        sdk.dashboard.return_value = serialize.deserialize(
            json.dumps(dashboard_data), models.Dashboard)

        dashboard = self.__scraper.scrape_dashboard('dashboard-id')

        self.assertEqual('dashboard-id', dashboard.id)
        sdk.dashboard.assert_called_once()
        sdk.dashboard.assert_called_with(dashboard_id='dashboard-id')

    def test_scrape_dashboard_should_raise_sdk_error_on_failure(self):
        sdk = self.__scraper.__dict__['_MetadataScraper__sdk']
        sdk.dashboard.side_effect = error.SDKError('SDK error')

        self.assertRaises(error.SDKError, self.__scraper.scrape_dashboard,
                          'dashboard-id')
        sdk.dashboard.assert_called_once()
        sdk.dashboard.assert_called_with(dashboard_id='dashboard-id')

    def test_scrape_all_dashboards_should_return_list(self):
        sdk = self.__scraper.__dict__['_MetadataScraper__sdk']

        dashboard_data = {
            'id': 'dashboard-id',
            'title': 'A B C',
            'space': {
                'name': 'Test folder',
                'parent_id': '',
            },
        }

        sdk.search_dashboards.return_value = [
            serialize.deserialize(json.dumps(dashboard_data), models.Dashboard)
        ]

        dashboards = self.__scraper.scrape_all_dashboards()

        self.assertEqual(1, len(dashboards))
        self.assertEqual('dashboard-id', dashboards[0].id)
        sdk.search_dashboards.assert_called_once()
        sdk.search_dashboards.assert_called_with(
            fields='id,title,created_at,description,space,hidden,user_id,'
            'view_count,favorite_count,last_accessed_at,last_viewed_at,'
            'deleted,deleted_at,deleter_id,dashboard_elements')

    def test_scrape_dashboards_from_folder_should_return_list(self):
        sdk = self.__scraper.__dict__['_MetadataScraper__sdk']

        dashboard_data = {
            'id': 'dashboard-id',
            'title': 'A B C',
        }

        sdk.search_dashboards.return_value = [
            serialize.deserialize(json.dumps(dashboard_data), models.Dashboard)
        ]

        folder_data = {
            'id': 'folder-id',
            'name': 'Test folder',
            'parent_id': '',
        }

        dashboards = self.__scraper.scrape_dashboards_from_folder(
            serialize.deserialize(json.dumps(folder_data), models.Folder))

        self.assertEqual(1, len(dashboards))
        self.assertEqual('dashboard-id', dashboards[0].id)
        sdk.search_dashboards.assert_called_once()
        sdk.search_dashboards.assert_called_with(
            space_id='folder-id',
            fields='id,title,created_at,description,space,hidden,user_id,'
            'view_count,favorite_count,last_accessed_at,last_viewed_at,'
            'deleted,deleted_at,deleter_id,dashboard_elements')

    def test_scrape_folder_should_return_object(self):
        sdk = self.__scraper.__dict__['_MetadataScraper__sdk']

        folder_data = {
            'id': 'folder-id',
            'name': 'Test folder',
            'parent_id': '',
        }

        sdk.folder.return_value = serialize.deserialize(
            json.dumps(folder_data), models.Folder)

        folder = self.__scraper.scrape_folder('folder-id')

        self.assertEqual('folder-id', folder.id)
        sdk.folder.assert_called_once()
        sdk.folder.assert_called_with(
            folder_id='folder-id',
            fields='id,name,parent_id,child_count,creator_id,dashboards,looks')

    def test_scrape_all_folders_should_return_list(self):
        sdk = self.__scraper.__dict__['_MetadataScraper__sdk']

        folder_data = {
            'id': 'folder-id',
            'name': 'Folder name',
            'parent_id': '',
        }

        sdk.search_folders.return_value = [
            serialize.deserialize(json.dumps(folder_data), models.Folder)
        ]

        folders = self.__scraper.scrape_all_folders()

        self.assertEqual(1, len(folders))
        self.assertEqual('folder-id', folders[0].id)
        sdk.search_folders.assert_called_once()
        sdk.search_folders.assert_called_with(
            fields='id,name,parent_id,child_count,creator_id')

    def test_scrape_top_level_folders_should_return_list(self):
        sdk = self.__scraper.__dict__['_MetadataScraper__sdk']

        folder_data = {
            'id': 'folder-id',
            'name': 'Folder name',
            'parent_id': '',
        }

        sdk.search_folders.return_value = [
            serialize.deserialize(json.dumps(folder_data), models.Folder)
        ]

        folders = self.__scraper.scrape_top_level_folders()

        self.assertEqual(1, len(folders))
        self.assertEqual('folder-id', folders[0].id)
        sdk.search_folders.assert_called_once()
        sdk.search_folders.assert_called_with(
            fields='id,name,parent_id,child_count,creator_id',
            parent_id='IS NULL')

    def test_scrape_child_folders_should_return_list(self):
        sdk = self.__scraper.__dict__['_MetadataScraper__sdk']

        folder_data = {
            'id': 'folder-id',
            'name': 'Folder name',
            'parent_id': '',
        }

        sdk.folder_children.return_value = [
            serialize.deserialize(json.dumps(folder_data), models.Folder)
        ]

        parent_data = {
            'id': 'parent-folder-id',
            'name': 'Parent folder name',
            'parent_id': '',
        }

        folders = self.__scraper.scrape_child_folders(
            serialize.deserialize(json.dumps(parent_data), models.Folder))

        self.assertEqual(1, len(folders))
        self.assertEqual('folder-id', folders[0].id)
        sdk.folder_children.assert_called_once()
        sdk.folder_children.assert_called_with(
            folder_id='parent-folder-id',
            fields='id,name,parent_id,child_count,creator_id')

    def test_scrape_look_should_return_object_on_success(self):
        sdk = self.__scraper.__dict__['_MetadataScraper__sdk']

        look_data = {
            'id': 123,
        }

        sdk.look.return_value = serialize.deserialize(json.dumps(look_data),
                                                      models.Look)

        look = self.__scraper.scrape_look(123)

        self.assertEqual(123, look.id)
        sdk.look.assert_called_once()
        sdk.look.assert_called_with(look_id=123)

    def test_scrape_all_looks_should_return_list(self):
        sdk = self.__scraper.__dict__['_MetadataScraper__sdk']

        look_data = {
            'id': 123,
            'title': 'A B C',
            'space': {
                'name': 'Test folder',
                'parent_id': '',
            },
        }

        sdk.search_looks.return_value = [
            serialize.deserialize(json.dumps(look_data), models.Look)
        ]

        looks = self.__scraper.scrape_all_looks()

        self.assertEqual(1, len(looks))
        self.assertEqual(123, looks[0].id)
        sdk.search_looks.assert_called_once()
        sdk.search_looks.assert_called_with(
            fields='id,title,created_at,updated_at,description,space,public,'
            'user_id,last_updater_id,query_id,url,short_url,public_url,'
            'excel_file_url,google_spreadsheet_formula,view_count,'
            'favorite_count,last_accessed_at,last_viewed_at,deleted,'
            'deleter_id')

    def test_scrape_looks_from_folder_should_return_list(self):
        sdk = self.__scraper.__dict__['_MetadataScraper__sdk']

        look_data = {
            'id': 123,
            'title': 'A B C',
        }

        sdk.search_looks.return_value = [
            serialize.deserialize(json.dumps(look_data), models.Look)
        ]

        folder_data = {
            'id': 'folder-id',
            'name': 'Test folder',
            'parent_id': '',
        }
        looks = self.__scraper.scrape_looks_from_folder(
            serialize.deserialize(json.dumps(folder_data), models.Folder))

        self.assertEqual(1, len(looks))
        self.assertEqual(123, looks[0].id)
        sdk.search_looks.assert_called_once()
        sdk.search_looks.assert_called_with(
            space_id='folder-id',
            fields='id,title,created_at,updated_at,description,space,public,'
            'user_id,last_updater_id,query_id,url,short_url,public_url,'
            'excel_file_url,google_spreadsheet_formula,view_count,'
            'favorite_count,last_accessed_at,last_viewed_at,deleted,'
            'deleter_id')

    def test_scrape_query_should_return_object(self):
        sdk = self.__scraper.__dict__['_MetadataScraper__sdk']

        query_data = {
            'id': 123,
            'model': '',
            'view': '',
        }

        sdk.query.return_value = serialize.deserialize(json.dumps(query_data),
                                                       models.Query)

        query = self.__scraper.scrape_query(123)

        self.assertEqual(123, query.id)
        sdk.query.assert_called_once()
        sdk.query.assert_called_with(query_id=123)

    def test_scrape_query_generated_sql_should_return_string(self):
        sdk = self.__scraper.__dict__['_MetadataScraper__sdk']
        sdk.run_query.return_value = 'select *'

        sql = self.__scraper.scrape_query_generated_sql(123)

        self.assertEqual('select *', sql)
        sdk.run_query.assert_called_once()
        sdk.run_query.assert_called_with(query_id=123, result_format='sql')

    def test_scrape_model_explore_should_return_object_on_success(self):
        sdk = self.__scraper.__dict__['_MetadataScraper__sdk']

        model_explore_data = {
            'project_name': 'test-project',
            'connection_name': 'test-connection',
        }

        sdk.lookml_model_explore.return_value = serialize.deserialize(
            json.dumps(model_explore_data), models.LookmlModelExplore)

        model = self.__scraper.scrape_lookml_model_explore(
            'test-model', 'test-view')

        self.assertEqual('test-project', model.project_name)
        sdk.lookml_model_explore.assert_called_once()
        sdk.lookml_model_explore.assert_called_with(
            lookml_model_name='test-model', explore_name='test-view')

    def test_scrape_model_explore_should_raise_sdk_error_on_failure(self):
        sdk = self.__scraper.__dict__['_MetadataScraper__sdk']
        sdk.lookml_model_explore.side_effect = error.SDKError('SDK error')

        self.assertRaises(error.SDKError,
                          self.__scraper.scrape_lookml_model_explore,
                          'test-model', 'test-view')
        sdk.lookml_model_explore.assert_called_once()
        sdk.lookml_model_explore.assert_called_with(
            lookml_model_name='test-model', explore_name='test-view')

    def test_scrape_connection_should_return_object(self):
        sdk = self.__scraper.__dict__['_MetadataScraper__sdk']

        connection_data = {
            'name': 'test-connection',
        }

        sdk.connection.return_value = serialize.deserialize(
            json.dumps(connection_data), models.DBConnection)

        connection = self.__scraper.scrape_connection('test-connection')

        self.assertEqual('test-connection', connection.name)
        sdk.connection.assert_called_once()
        sdk.connection.assert_called_with(connection_name='test-connection')
