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
        return self.__qrs_api_helper.get_full_app_list()

    def scrape_all_streams(self):
        return self.__qrs_api_helper.get_full_stream_list()

    def scrape_sheets(self, app_metadata):
        sheets = self.__engine_api_helper.get_sheets(app_metadata.get('id'))
        return sheets if sheets else []
