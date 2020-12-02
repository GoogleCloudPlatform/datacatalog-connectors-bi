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

import logging

from google.datacatalog_connectors.qlik.scrape import \
    engine_api_helper, repository_services_api_helper


class MetadataScraper:
    """A Facade that provides a simplified interface for the Qlik services,
    comprising the interactions between Qlik Sense Proxy Service (QPS), Qlik
    Sense Repository Service (QRS), and Qlik Engine JSON API.

    """

    def __init__(self, server_address, ad_domain, username, password):
        self.__qrs_api_helper = \
            repository_services_api_helper.RepositoryServicesAPIHelper(
                server_address, ad_domain, username, password)
        self.__engine_api_helper = engine_api_helper.EngineAPIHelper(
            server_address, ad_domain, username, password)

    def scrape_all_apps(self):
        self.__log_scrape_start('Scraping all Apps...')
        apps = self.__qrs_api_helper.get_full_app_list()

        logging.info('  %s Apps found:', len(apps))
        for app in apps:
            logging.info(
                '    - %s/%s [%s]',
                app.get('stream').get('name')
                if app.get('published') else 'NOT PUBLISHED', app.get('name'),
                app.get('id'))

        return apps

    def scrape_all_streams(self):
        self.__log_scrape_start('Scraping all Streams...')
        streams = self.__qrs_api_helper.get_full_stream_list()

        logging.info('  %s Streams found:', len(streams))
        for stream in streams:
            logging.info('    - %s [%s]', stream.get('name'), stream.get('id'))

        return streams

    def scrape_sheets(self, app_metadata):
        self.__log_scrape_start('Scraping Sheets from "%s"',
                                app_metadata.get('name'))
        sheets = self.__engine_api_helper.get_sheets(app_metadata.get('id'))
        sheets = sheets if sheets else []

        logging.info('  %s Sheets found:', len(sheets))
        for sheet in sheets:
            logging.info('    - %s [%s]',
                         sheet.get('qMeta').get('title'),
                         sheet.get('qInfo').get('qId'))

        return sheets

    @classmethod
    def __log_scrape_start(cls, message, *args):
        logging.info('')
        logging.info(message, *args)
        logging.info('-------------------------------------------------')
