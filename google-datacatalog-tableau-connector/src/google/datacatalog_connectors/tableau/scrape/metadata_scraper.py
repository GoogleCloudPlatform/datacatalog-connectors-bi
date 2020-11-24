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
                 site=None):

        self.__server_address = server_address
        self.__api_version = api_version
        self.__username = username
        self.__password = password
        self.__site = site

    def scrape_dashboards(self, query_filter=None):
        return self.__scrape_metadata(self.__scrape_dashboards, query_filter)

    def scrape_sites(self, query_filter=None):
        return self.__scrape_metadata(self.__scrape_sites, query_filter)

    def scrape_workbooks(self, query_filter=None):
        return self.__scrape_metadata(self.__scrape_workbooks, query_filter)

    def __scrape_metadata(self, reader_method, query_filter):
        metadata = []

        sites = self.__get_sites()
        if len(sites) == 0:
            return metadata

        for site in sites:
            auth_credentials = authenticator.Authenticator.authenticate(
                self.__server_address, self.__api_version, self.__username,
                self.__password, site)
            metadata.extend(
                reader_method(
                    metadata_api_helper.MetadataAPIHelper(
                        self.__server_address, auth_credentials),
                    query_filter))

        return metadata

    @classmethod
    def __scrape_dashboards(cls, metadata_reader, query_filter):
        return metadata_reader.fetch_dashboards(query_filter)

    @classmethod
    def __scrape_sites(cls, metadata_reader, query_filter):
        return metadata_reader.fetch_sites(query_filter)

    @classmethod
    def __scrape_workbooks(cls, metadata_reader, query_filter):
        return metadata_reader.fetch_workbooks(query_filter)

    def __get_sites(self):
        if self.__site:
            sites = [self.__site]
        else:
            default_site_credentials = \
                authenticator.Authenticator.authenticate(
                    self.__server_address,
                    self.__api_version,
                    self.__username,
                    self.__password)
            available_sites = rest_api_helper.RestAPIHelper(
                server_address=self.__server_address,
                api_version=self.__api_version,
                auth_credentials=default_site_credentials). \
                get_all_sites_on_server()
            sites = [
                available_site['contentUrl']
                for available_site in available_sites
            ]

        return sites
