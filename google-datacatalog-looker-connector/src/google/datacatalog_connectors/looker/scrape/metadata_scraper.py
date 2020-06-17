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

from functools import lru_cache
import logging

from looker_sdk import client, error


class MetadataScraper:
    __DASHBOARD_FIELDS = 'id,title,created_at,description,space,hidden,' \
                         'user_id,view_count,favorite_count,' \
                         'last_accessed_at,last_viewed_at,deleted,' \
                         'deleted_at,deleter_id,dashboard_elements'
    __FOLDER_FIELDS = 'id,name,parent_id,child_count,creator_id'
    __LOOK_FIELDS = 'id,title,created_at,updated_at,description,space,' \
                    'public,user_id,last_updater_id,query_id,url,short_url,' \
                    'public_url,excel_file_url,google_spreadsheet_formula,' \
                    'view_count,favorite_count,last_accessed_at,' \
                    'last_viewed_at,deleted,deleter_id'

    def __init__(self, looker_credentials_file):
        self.__sdk = client.setup(looker_credentials_file)

    def scrape_dashboard(self, dashboard_id):
        self.__log_scrape_start('Scraping dashboard by id: %s...',
                                dashboard_id)

        try:
            dashboard = self.__sdk.dashboard(dashboard_id=dashboard_id)
            self.__log_single_object_scrape_result(dashboard)
        except error.SDKError as e:
            logging.info('API call failed...')
            logging.info(e)
            raise

        return dashboard

    def scrape_all_dashboards(self):
        self.__log_scrape_start('Scraping all dashboards...')

        # The all_dashboards method response does not include all fields the
        # connector actually needs, so search_dashboards is used here.
        #
        # Please notice "lookml" dashboards are not included in
        # search_dashboards response and need a special handling.
        dashboards = self.__sdk.search_dashboards(
            fields=self.__DASHBOARD_FIELDS)

        logging.info('%s dashboards found:', len(dashboards))
        for dashboard in dashboards:
            logging.info('%s/%s [%s]', dashboard.space.name, dashboard.title,
                         dashboard.id)

        return dashboards

    def scrape_dashboards_from_folder(self, folder):
        self.__log_scrape_start('Scraping "%s" folder dashboards...',
                                folder.name)
        dashboards = self.__sdk.search_dashboards(
            space_id=folder.id, fields=self.__DASHBOARD_FIELDS)

        logging.info('%s dashboards found:', len(dashboards))
        for dashboard in dashboards:
            logging.info('%s [%s]', dashboard.title, dashboard.id)

        return dashboards

    def scrape_folder(self, folder_id):
        self.__log_scrape_start('Scraping folder by id: %s...', folder_id)
        folder = self.__sdk.folder(
            folder_id=folder_id,
            fields=f'{self.__FOLDER_FIELDS},dashboards,looks')
        self.__log_single_object_scrape_result(folder)
        return folder

    def scrape_all_folders(self):
        self.__log_scrape_start('Scraping all folders...')

        # The all_folders method response does not include all fields the
        # connector actually needs, so search_folders is used here.
        #
        # Also, empty folders are not included in all_folders response.
        folders = self.__sdk.search_folders(fields=self.__FOLDER_FIELDS)

        logging.info('%s folders found:', len(folders))
        for folder in folders:
            logging.info('%s [%s]', folder.name, folder.id)

        return folders

    def scrape_top_level_folders(self):
        self.__log_scrape_start('Scraping top-level folders...')
        folders = self.__sdk.search_folders(fields=self.__FOLDER_FIELDS,
                                            parent_id='IS NULL')

        logging.info('%s folders found:', len(folders))
        for folder in folders:
            logging.info('/%s [%s]', folder.name, folder.id)

        return folders

    def scrape_child_folders(self, parent_folder):
        self.__log_scrape_start('Scraping "%s" children...',
                                parent_folder.name)
        folders = self.__sdk.folder_children(folder_id=parent_folder.id,
                                             fields=self.__FOLDER_FIELDS)

        logging.info('%s folders found:', len(folders))
        for folder in folders:
            logging.info('%s/%s [%s]', parent_folder.name, folder.name,
                         folder.id)

        return folders

    def scrape_look(self, look_id):
        self.__log_scrape_start('Scraping look by id: %s...', look_id)
        look = self.__sdk.look(look_id=look_id)
        self.__log_single_object_scrape_result(look)
        return look

    def scrape_all_looks(self):
        self.__log_scrape_start('Scraping all looks...')

        # The all_looks method response does not include all fields the
        # connector actually needs, so search_looks is used here.
        looks = self.__sdk.search_looks(fields=self.__LOOK_FIELDS)

        logging.info('%s looks found:', len(looks))
        for look in looks:
            logging.info('%s/%s [%s]', look.space.name, look.title, look.id)

        return looks

    def scrape_looks_from_folder(self, folder):
        self.__log_scrape_start('Scraping "%s" folder looks...', folder.name)
        looks = self.__sdk.search_looks(space_id=folder.id,
                                        fields=self.__LOOK_FIELDS)

        logging.info('%s looks found:', len(looks))
        for look in looks:
            logging.info('%s [%s]', look.title, look.id)

        return looks

    @lru_cache(maxsize=1024)
    def scrape_query(self, query_id):
        self.__log_scrape_start('Scraping query by id: %s...', query_id)
        query = self.__sdk.query(query_id=query_id)
        self.__log_single_object_scrape_result(query)
        return query

    @lru_cache(maxsize=1024)
    def scrape_query_generated_sql(self, query_id):
        self.__log_scrape_start('Scraping generated SQL by query id: %s...',
                                query_id)
        sql = self.__sdk.run_query(query_id=query_id, result_format='sql')
        self.__log_single_object_scrape_result(sql)
        return sql

    @lru_cache(maxsize=1024)
    def scrape_lookml_model_explore(self, model_name, explore_name):
        self.__log_scrape_start(
            'Scraping LookML model explore by name: %s/%s...', model_name,
            explore_name)

        try:
            model = self.__sdk.lookml_model_explore(
                lookml_model_name=model_name, explore_name=explore_name)
        except error.SDKError as e:
            logging.info('API call failed...')
            logging.info(e)
            raise

        self.__log_single_object_scrape_result(model)
        return model

    @lru_cache(maxsize=128)
    def scrape_connection(self, connection_name):
        self.__log_scrape_start('Scraping connection by name: %s...',
                                connection_name)
        connection = self.__sdk.connection(connection_name=connection_name)
        self.__log_single_object_scrape_result(connection)
        return connection

    @classmethod
    def __log_scrape_start(cls, message, *args):
        logging.info('')
        logging.info(message, *args)
        logging.info('-------------------------------------------------')

    @classmethod
    def __log_single_object_scrape_result(cls, the_object):
        logging.info('Found!' if the_object else 'NOT found!')
