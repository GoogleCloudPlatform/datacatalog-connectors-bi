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

import configparser
import logging
from urllib.parse import urlparse

from looker_sdk import error

from google.datacatalog_connectors.commons import cleanup, ingest

from google.datacatalog_connectors.looker import entities, prepare, scrape
from google.datacatalog_connectors.looker.prepare import constant


class MetadataSynchronizer:
    __ENTRY_GROUP_ID = 'looker'
    __SPECIFIED_SYSTEM = 'looker'

    def __init__(self, datacatalog_project_id, datacatalog_location_id,
                 looker_credentials_file):

        self.__project_id = datacatalog_project_id
        self.__location_id = datacatalog_location_id

        self.__metadata_scraper = scrape.MetadataScraper(
            looker_credentials_file)

        self.__tag_template_factory = prepare.DataCatalogTagTemplateFactory(
            project_id=datacatalog_project_id,
            location_id=datacatalog_location_id)

        self.__instance_url = self.__extract_instance_url(
            looker_credentials_file)

        self.__assembled_entry_factory = prepare.AssembledEntryFactory(
            project_id=datacatalog_project_id,
            location_id=datacatalog_location_id,
            entry_group_id=self.__ENTRY_GROUP_ID,
            user_specified_system=self.__SPECIFIED_SYSTEM,
            instance_url=self.__instance_url)

    @classmethod
    def __extract_instance_url(cls, credentials_file):
        config_parser = configparser.ConfigParser()
        config_parser.read(credentials_file)
        api_url = config_parser['Looker']['base_url']

        parsed_uri = urlparse(api_url)
        scheme = parsed_uri.scheme

        # Strip the port number.
        server_address = parsed_uri.netloc[:parsed_uri.netloc.find(':')]

        return f'{scheme}://{server_address}'

    def run(self):
        """Coordinates a full scrape > prepare > ingest process."""

        # Scrape metadata from Looker server.
        logging.info('')
        logging.info('===> Scraping Looker metadata...')

        logging.info('Folders...')
        folders_dict = self.__scrape_folders()

        logging.info('')
        logging.info('Queries...')
        queries_dict = self.__scrape_queries(folders_dict)
        logging.info('==== DONE ========================================')

        # Prepare: convert Looker metadata into Data Catalog entities model.
        logging.info('')
        logging.info('===> Converting Looker metadata'
                     ' into Data Catalog entities model...')

        tag_templates_dict = self.__make_tag_templates_dict()

        assembled_entries_dict = self.__make_assembled_entries_dict(
            folders_dict, queries_dict, tag_templates_dict)
        logging.info('==== DONE ========================================')

        # Data Catalog entries relationship mapping.
        logging.info('')
        logging.info('===> Mapping Data Catalog entries relationships...')

        self.__map_datacatalog_relationships(assembled_entries_dict)
        logging.info('==== DONE ========================================')

        # Data Catalog clean up: delete obsolete data.
        logging.info('')
        logging.info('===> Deleting Data Catalog obsolete metadata...')

        self.__delete_obsolete_entries(assembled_entries_dict)
        logging.info('==== DONE ========================================')

        # Ingest metadata into Data Catalog.
        logging.info('')
        logging.info('===> Synchronizing Looker :: Data Catalog metadata...')

        self.__ingest_metadata(tag_templates_dict, assembled_entries_dict)
        logging.info('==== DONE ========================================')

    def __scrape_folders(self):
        """
        Scrape metadata from all folders belonging to a given Looker instance.
        Folders metadata include nested objects such as sub-folders,
        dashboards, dashboard elements (aka tiles), and looks.

        :return: A ``dict`` in which keys are top-level folders IDs
            and values are lists containing all metadata gathered from that
            folders in a deep-first hierarchy.
        """
        all_folders = self.__metadata_scraper.scrape_all_folders()
        all_dashboards = self.__metadata_scraper.scrape_all_dashboards()
        all_looks = self.__metadata_scraper.scrape_all_looks()

        top_level_folders = [
            folder for folder in all_folders
            if (folder.parent_id is None or folder.parent_id == '' or
                folder.parent_id == 'None')
        ]

        folders_dict = {}
        for folder in top_level_folders:
            folders_dict[folder.id] = self.__scrape_folder_from_flat_lists(
                folder, all_folders, all_dashboards, all_looks)

        # Explict "lookml" folder handling.
        # This special folder is not included in search_folders response
        # (see ``MetadataScraper.scrape_all_folders()`` for details).
        # Although all_folders response may include it, all_dashboards response
        # returns nothing when space_id=lookml, so it's necessary to iterate
        # through folder object's properties to get all metadata the connector
        # cares about.
        lookml_folder_id = 'lookml'
        lookml_folder = self.__metadata_scraper.scrape_folder(lookml_folder_id)
        if lookml_folder:
            folders_dict[lookml_folder_id] = \
                self.__scrape_folder_by_recursive_requests(lookml_folder)

        self.__log_folders_related_scraping_results(folders_dict)

        return folders_dict

    def __scrape_folder_from_flat_lists(self, folder, all_folders,
                                        all_dashboards, all_looks):
        """
        Retrieve folders metadata from the given flat asset lists.
        """
        if folder.dashboards is None:
            folder.dashboards = []

        folder.dashboards.extend([
            dashboard for dashboard in all_dashboards
            if dashboard.space.id == folder.id
        ])

        if folder.looks is None:
            folder.looks = []

        folder.looks.extend(
            [look for look in all_looks if look.space.id == folder.id])

        folders = [folder]

        child_folders = [
            child_folder for child_folder in all_folders
            if child_folder.parent_id == folder.id
        ]

        # TODO Check whether the already used folders, dashboards, and looks
        #  can be removed from the given lists to improve next iterations
        #  performance and memory usage.

        for folder in child_folders:
            folders.extend(
                self.__scrape_folder_from_flat_lists(folder, all_folders,
                                                     all_dashboards,
                                                     all_looks))

        return folders

    def __scrape_folder_by_recursive_requests(self, folder):
        """
        Scrape the given folder metadata and do the same for all of its
        children by recursively requesting their information.
        """
        if folder.dashboards is None:
            folder.dashboards = []

        dashboards_ids = [dashboard.id for dashboard in folder.dashboards]
        folder.dashboards.clear()
        for dashboard_id in dashboards_ids:
            try:
                folder.dashboards.append(
                    self.__metadata_scraper.scrape_dashboard(dashboard_id))
            except error.SDKError:
                pass

        if folder.looks is None:
            folder.looks = []

        looks_ids = [look.id for look in folder.looks]
        folder.looks.clear()
        for look_id in looks_ids:
            folder.looks.append(self.__metadata_scraper.scrape_look(look_id))

        folders = [folder]

        child_folders = self.__metadata_scraper.scrape_child_folders(folder)
        for folder in child_folders:
            folders.extend(self.__scrape_folder_by_recursive_requests(folder))

        return folders

    @classmethod
    def __log_folders_related_scraping_results(cls, folders_dict):
        folders_count = 0
        dashboards_count = 0
        elements_count = 0
        looks_count = 0

        for folders in folders_dict.values():
            folders_count += len(folders)
            for folder in folders:
                if folder.dashboards:
                    dashboards_count += len(folder.dashboards)
                    for dashboard in folder.dashboards:
                        if dashboard.dashboard_elements:
                            elements_count += len(dashboard.dashboard_elements)
                looks_count += len(folder.looks) if folder.looks else 0

        assets_count = sum(
            [folders_count, dashboards_count, elements_count, looks_count])
        assets_count_str_len = len(str(assets_count))

        logging.info('')
        logging.info('==== %s folders-related assets scraped!', assets_count)
        spaces_count = assets_count_str_len - len(str(folders_count))
        logging.info('   > %s%s folders', " " * spaces_count, folders_count)
        spaces_count = assets_count_str_len - len(str(dashboards_count))
        logging.info('   > %s%s dashboards', " " * spaces_count,
                     dashboards_count)
        spaces_count = assets_count_str_len - len(str(elements_count))
        logging.info('   > %s%s dashboard elements', " " * spaces_count,
                     elements_count)
        spaces_count = assets_count_str_len - len(str(looks_count))
        logging.info('   > %s%s looks', " " * spaces_count, looks_count)

    def __scrape_queries(self, folders_dict):
        """
        Scrape metadata from all queries related to the given folders nested
        assets. A query metadata set includes its generated SQL statement,
        related LookML explore, and connection.

        :return: A ``dict`` in which keys are equals to the folders_dict keys
            and values are lists of ``entities.AssembledQueryMetadata``
            containing queries metadata gathered from assets nested to each
            of the "key" folders.
        """
        queries_dict = {}
        for folder_id, folders in folders_dict.items():
            query_ids = self.__get_folders_related_query_ids(folders)
            queries_dict[folder_id] = \
                [self.__scrape_query(query_id) for query_id in query_ids]

        self.__log_queries_related_scraping_results(queries_dict)

        return queries_dict

    @classmethod
    def __get_folders_related_query_ids(cls, folders):
        """
        :return: A ``set`` with all query IDs related to folders nested assets.
        """
        query_ids = set()
        for folder in folders:
            query_ids.update(cls.__get_folder_related_query_ids(folder))

        return query_ids

    @classmethod
    def __get_folder_related_query_ids(cls, folder):
        """
        Query IDs are found over folder dashboard elements and looks.

        :return: A ``set`` with all query IDs related to folder nested assets.
        """
        query_ids = set()

        for dashboard in folder.dashboards:
            query_ids.update([
                element.query_id
                for element in dashboard.dashboard_elements
                if element.query_id
            ])
            query_ids.update([
                element.result_maker.query_id
                for element in dashboard.dashboard_elements
                if element.result_maker and element.result_maker.query_id
            ])

        query_ids.update(
            [look.query_id for look in folder.looks if look.query_id])

        return query_ids

    def __scrape_query(self, query_id):
        query = self.__metadata_scraper.scrape_query(query_id)

        model_explore = None
        connection = None
        generated_sql = None

        try:
            model_explore = self.__metadata_scraper\
                .scrape_lookml_model_explore(query.model, query.view)
            connection = self.__metadata_scraper.scrape_connection(
                model_explore.connection_name)
            generated_sql = \
                self.__metadata_scraper.scrape_query_generated_sql(query_id)
        except error.SDKError:
            pass

        return entities.AssembledQueryMetadata(query, generated_sql,
                                               model_explore, connection)

    @classmethod
    def __log_queries_related_scraping_results(cls, queries_dict):
        assets_count = sum([len(queries) for queries in queries_dict.values()])
        assets_count_str_len = len(str(assets_count))

        unique_ids_count = len(
            set([
                assembled.query.id
                for queries in queries_dict.values()
                for assembled in queries
            ]))

        logging.info('')
        logging.info('==== %s queries scraped!', assets_count)
        spaces_count = assets_count_str_len - len(str(unique_ids_count))
        logging.info('   > %s%s are unique', " " * spaces_count,
                     unique_ids_count)

    def __make_tag_templates_dict(self):
        return {
            constant.TAG_TEMPLATE_ID_DASHBOARD:
                self.__tag_template_factory.make_tag_template_for_dashboard(),
            constant.TAG_TEMPLATE_ID_DASHBOARD_ELEMENT:
                self.__tag_template_factory.
                make_tag_template_for_dashboard_element(),
            constant.TAG_TEMPLATE_ID_FOLDER:
                self.__tag_template_factory.make_tag_template_for_folder(),
            constant.TAG_TEMPLATE_ID_LOOK:
                self.__tag_template_factory.make_tag_template_for_look(),
            constant.TAG_TEMPLATE_ID_QUERY:
                self.__tag_template_factory.make_tag_template_for_query(),
        }

    def __make_assembled_entries_dict(self, folders_dict, queries_dict,
                                      tag_templates_dict):
        """
        Make Data Catalog entries and tags for assets belonging to a given
        Looker instance.

        :return: A ``dict`` in which keys are equals to the folders_dict keys
            and values are flat lists containing assembled objects with all
            their related entries and tags.
        """
        assembled_entries = {}

        for folder_id, folders in folders_dict.items():
            assembled_entries[folder_id] = self.__assembled_entry_factory\
                .make_assembled_entries_list(
                    folders, queries_dict[folder_id], tag_templates_dict)

        return assembled_entries

    @classmethod
    def __map_datacatalog_relationships(cls, assembled_entries_dict):
        all_assembled_entries = []
        for assembled_entries_data in assembled_entries_dict.values():
            all_assembled_entries.extend(assembled_entries_data)

        prepare.EntryRelationshipMapper().fulfill_tag_fields(
            all_assembled_entries)

    def __delete_obsolete_entries(self, new_assembled_entries_dict):
        all_assembled_entries = []
        for assembled_entry_data in new_assembled_entries_dict.values():
            all_assembled_entries.extend(assembled_entry_data)

        cleanup.DataCatalogMetadataCleaner(
            self.__project_id, self.__location_id, self.__ENTRY_GROUP_ID).\
            delete_obsolete_metadata(
                all_assembled_entries,
                f'system={self.__SPECIFIED_SYSTEM}'
                f' tag:instance_url:{self.__instance_url}')

    def __ingest_metadata(self, tag_templates_dict, assembled_entries_dict):
        metadata_ingestor = ingest.DataCatalogMetadataIngestor(
            self.__project_id, self.__location_id, self.__ENTRY_GROUP_ID)

        for assembled_entries_data in assembled_entries_dict.values():
            metadata_ingestor.ingest_metadata(assembled_entries_data,
                                              tag_templates_dict)

        entries_count = sum(
            len(entries) for entries in assembled_entries_dict.values())
        logging.info('==== %s entries synchronized!', entries_count)
