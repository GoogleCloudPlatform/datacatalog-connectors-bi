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

from google.datacatalog_connectors.tableau.scrape import \
    authenticator, metadata_api_helper, rest_api_helper


class MetadataScraper:

    def __init__(self,
                 server_address,
                 api_version,
                 username,
                 password,
                 site_content_url=None):

        self.__server_address = server_address
        self.__api_version = api_version
        self.__username = username
        self.__password = password
        self.__site_content_url = site_content_url

        self.__site_content_urls = [site_content_url]

    def scrape_dashboards(self, query_filter=None):
        return self.__scrape_metadata(self.__scrape_dashboards, query_filter)

    def scrape_sites(self, query_filter=None):
        return self.__scrape_metadata(self.__scrape_sites, query_filter)

    def scrape_workbooks(self, query_filter=None):
        return self.__scrape_metadata(self.__scrape_workbooks, query_filter)

    def __scrape_metadata(self, reader_method, query_filter):
        metadata = []

        self.__initialize_site_content_urls()
        if not self.__site_content_urls:
            return metadata

        api_helper = metadata_api_helper.MetadataAPIHelper(
            self.__server_address)

        for site_content_url in self.__site_content_urls:
            auth_credentials = authenticator.Authenticator.authenticate(
                self.__server_address, self.__api_version, self.__username,
                self.__password, site_content_url)
            metadata.extend(
                reader_method(api_helper, auth_credentials, query_filter))

        return metadata

    @classmethod
    def __scrape_dashboards(cls, api_helper, auth_credentials, query_filter):
        return api_helper.fetch_dashboards(auth_credentials, query_filter)

    @classmethod
    def __scrape_sites(cls, api_helper, auth_credentials, query_filter):
        return api_helper.fetch_sites(auth_credentials, query_filter)

    @classmethod
    def __scrape_workbooks(cls, api_helper, auth_credentials, query_filter):
        return api_helper.fetch_workbooks(auth_credentials, query_filter)

    def __initialize_site_content_urls(self):
        # Single site scraping
        if self.__site_content_urls:
            return

        # Multiple site scraping (only for Tableau Server)
        default_site_credentials = \
            authenticator.Authenticator.authenticate(
                self.__server_address,
                self.__api_version,
                self.__username,
                self.__password)

        api_helper = rest_api_helper.RestAPIHelper(self.__server_address,
                                                   self.__api_version)
        available_sites = api_helper.get_all_sites_on_server(
            default_site_credentials)
        self.__site_content_urls.extend(
            [site['contentUrl'] for site in available_sites])
