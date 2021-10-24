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

import logging
from typing import Dict, List, Tuple

from google.cloud.datacatalog import Entry, Tag

from google.datacatalog_connectors.sisense.addons import \
    elasticube_dependency_finder as base_finder
from google.datacatalog_connectors.sisense.prepare import constants


class LinkedResourceBasedFinder(base_finder.ElastiCubeDependencyFinder):
    """Count on Google Data Catalog search capabilities to find
    ElastiCube-related dependencies using Entry's ``linked_resource`` field.

    The public methods are intended to provide clients a simplified interface
    for Data Catalog search and results handling.
    """

    def find(self,
             sisense_asset_url: str) -> Dict[str, Tuple[Entry, List[Tag]]]:
        """
        Orchestrate actions that comprise:
            - performing a catalog lookup entry operation;
            - getting Tags based on the lookup results;
            - filtering Tags that contain Entry enrichment or JAQL metadata;
            - assembling the Entries and Tags in a user-friendly dict.

        Returns:
            dict: Keys are Entry names and values are tuples in which the
            first element is an Entry object and the second is an array of
            ElastiCube-related Tags.

        Raises:
            Exception: If no Sisense asset URL provided.
        """
        if not sisense_asset_url:
            raise Exception('A Sisense asset URL must be provided.')

        if sisense_asset_url.endswith('/'):
            sisense_asset_url = sisense_asset_url[:len(sisense_asset_url) - 1]

        asset_id = sisense_asset_url[sisense_asset_url.rfind('/') + 1:]

        # ========================================
        # Catalog search fallback start
        logging.info('')
        logging.info('The entries.lookup API does not work for user-specified'
                     ' Entries'
                     ' (https://issuetracker.google.com/issues/202510978'
                     ' for details). Catalog search is used as a fallback.')

        types = [
            constants.USER_SPECIFIED_TYPE_DASHBOARD,
            constants.USER_SPECIFIED_TYPE_WIDGET
        ]
        query = self._make_query(types, asset_id=asset_id)
        tmp_entry_names = self._search_catalog(query)

        if not tmp_entry_names:
            logging.info('')
            logging.info('Not found!')
            return {}

        logging.info('')
        logging.info('===> Getting entries...')
        tmp_entries_dict = self._get_entries(tmp_entry_names)
        logging.info('==== DONE ========================================')

        entry = next((entry for entry in tmp_entries_dict.values()
                      if entry.linked_resource == sisense_asset_url), None)
        # Catalog search fallback end
        # ========================================

        # The above catalog search fallback should be replaced with the
        # commented lines below if
        # https://issuetracker.google.com/issues/202510978 gets fixed.
        #
        # logging.info('')
        # logging.info('===> Looking for the linked catalog entry...')
        # entry = self.__datacatalog_facade.lookup_entry(sisense_asset_url)
        # logging.info('==== DONE ========================================')

        if not entry:
            logging.info('')
            logging.info('Not found!')
            return {}

        entry_names = [entry.name]
        entries_dict = {entry.name: entry}

        dashboard_type = constants.USER_SPECIFIED_TYPE_DASHBOARD
        if entry.user_specified_type == dashboard_type:
            # ========================================
            # Catalog search fallback start

            # If the asset is a Dashboard, search results include the Widgets
            # that belong to it, thus there's no need to perform a second
            # search. It happens because the `asset_id`-based search uses a
            # `tag:id:<val>` term which causes Data Catalog to return all
            # entries tagged with fields that have the `id` substring in their
            # names and values matching the provided Dashboard ID. Widgets'
            # extended metadata tags have a `dashboard_id` field, so they match
            # the above condition.
            widget_type = constants.USER_SPECIFIED_TYPE_WIDGET
            widget_entries_dict = {
                entry.name: entry
                for entry in tmp_entries_dict.values()
                if entry.user_specified_type == widget_type
            }
            widget_entry_names = widget_entries_dict.keys()
            # Catalog search fallback end
            # ========================================

            # The above catalog search fallback should be replaced with the
            # commented lines below if
            # https://issuetracker.google.com/issues/202510978 gets fixed.
            #
            # widgets_query = self.__make_query(
            #     [constants.USER_SPECIFIED_TYPE_WIDGET],
            #     dashboard_id=asset_id)
            # widget_entry_names = self.__search_catalog(widgets_query)
            # widget_entries_dict = self.__get_entries(widget_entry_names)

            entry_names.extend(widget_entry_names)
            entries_dict = {**entries_dict, **widget_entries_dict}

        logging.info('')
        logging.info('===> Listing tags...')
        tags_dict = self._list_tags(entry_names)
        logging.info('==== DONE ========================================')

        logging.info('')
        logging.info('===> Assembling results...')
        assembled_results = {}
        for entry_name in entry_names:
            entry = entries_dict.get(entry_name)
            tags = tags_dict.get(entry_name)
            assembled_results[entry_name] = (entry,
                                             self.__filter_relevant_tags(
                                                 entry, tags))
        logging.info('==== DONE ========================================')

        return assembled_results

    @classmethod
    def __filter_relevant_tags(cls, entry: Entry,
                               tags: List[Tag]) -> List[Tag]:
        """Filter Tags for ElastiCube dependencies listing, "Relevant", in this
        context, means Tags that comprise asset and JAQL metadata.

        Returns:
            list: The filtered Tags.
        """
        filtered_tags = []

        asset_metadata_tag = cls.filter_asset_metadata_tag(tags)
        if asset_metadata_tag:
            filtered_tags.append(asset_metadata_tag)
        jaql_tags = cls.filter_jaql_tags(tags)
        filtered_tags.extend(
            cls._sort_tags_by_schema(entry.schema.columns, '', jaql_tags))

        return filtered_tags
