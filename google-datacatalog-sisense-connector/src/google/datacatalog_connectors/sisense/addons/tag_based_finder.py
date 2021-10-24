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
from typing import Dict, List, Optional, Tuple

from google.cloud.datacatalog import Entry, Tag

from google.datacatalog_connectors.sisense.addons import \
    elasticube_dependency_finder as base_finder
from google.datacatalog_connectors.sisense.prepare import constants


class TagBasedFinder(base_finder.ElastiCubeDependencyFinder):
    """Count on Google Data Catalog search capabilities to find
    ElastiCube-related dependencies using specific Tag fields.

    The public methods are intended to provide clients a simplified interface
    for Data Catalog search and results handling.
    """

    def find(
            self,
            datasource: Optional[str] = None,
            table: Optional[str] = None,
            column: Optional[str] = None
    ) -> Dict[str, Tuple[Entry, List[Tag]]]:
        """
        Orchestrate actions that comprise:
            - generating a query string to search Google Data Catalog;
            - performing a catalog search operation;
            - getting Entries and Tags based on search results;
            - filtering Tags that contain Entry enrichment metadata or match
                the table/column search criteria;
            - assembling the Entries and Tags in a user-friendly dict.

        Returns:
            dict: Keys are Entry names and values are tuples in which the
            first element is an Entry object and the second is an array of
            ElastiCube-related Tags.

        Raises:
            Exception: If no datasource, table, or column are provided.
        """
        if not (datasource or table or column):
            raise Exception(
                'Either a datasource, table, or column must be provided.')

        types = [
            constants.USER_SPECIFIED_TYPE_DASHBOARD,
            constants.USER_SPECIFIED_TYPE_WIDGET
        ]
        query = self._make_query(types,
                                 datasource=datasource,
                                 table=table,
                                 column=column)
        entry_names = self._search_catalog(query)

        if not entry_names:
            return {}

        logging.info('')
        logging.info('===> Getting entries...')
        entries_dict = self._get_entries(entry_names)
        logging.info('==== DONE ========================================')

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
                                                 entry, tags, table, column))
        logging.info('==== DONE ========================================')

        logging.info('')
        logging.info('%d assembled result(s) to be returned',
                     len(assembled_results))
        logging.info('')

        return assembled_results

    @classmethod
    def __filter_relevant_tags(cls,
                               entry: Entry,
                               tags: List[Tag],
                               table: Optional[str] = None,
                               column: Optional[str] = None) -> List[Tag]:
        """Filter Tags for ElastiCube dependencies finding. "Relevant", in this
        context, means Tags that have ElastiCube-related information such as
        data source, table, or column names that match the user-provided search
        criteria.

        The data source name, if provided as search criteria, is expected to be
        handled as a search term in the catalog search operation that precedes
        this step, therefore this method does not take it into account for
        filtering purposes.

        Returns:
            list: The filtered Tags.
        """
        filtered_tags = []

        asset_metadata_tag = cls.filter_asset_metadata_tag(tags)
        if asset_metadata_tag:
            filtered_tags.append(asset_metadata_tag)
        table_column_matching_tags = cls.__filter_table_column_matching_tags(
            tags, table, column)
        filtered_tags.extend(
            cls._sort_tags_by_schema(entry.schema.columns, '',
                                     table_column_matching_tags))

        return filtered_tags

    @classmethod
    def __filter_table_column_matching_tags(
            cls,
            tags: List[Tag],
            table: Optional[str] = None,
            column: Optional[str] = None) -> List[Tag]:
        """Filter JAQL-related Tags which `table` and/or `column` fields match
        the provided args.

        If both `table` and `column` args are provided, a given Tag's
        corresponding fields must match both values in order to have the Tag
        added to the resulting list.

        Returns:
            list: The filtered Tags.
        """
        jaql_tags = cls.filter_jaql_tags(tags)
        table_matching_tags = cls.__filter_table_matching_tags(
            jaql_tags, table)
        column_matching_tags = cls.__filter_column_matching_tags(
            table_matching_tags if table and column else tags, column)

        return column_matching_tags if column else table_matching_tags

    @classmethod
    def __filter_table_matching_tags(cls,
                                     tags: List[Tag],
                                     table: Optional[str] = None) -> List[Tag]:
        """Filter Tags that have a `table` field that match the provided table
        name.

        Returns:
            list: The filtered Tags.
        """
        if not table:
            return []

        table_field = 'table'
        table_lower = table.lower()

        return [
            tag for tag in tags or [] if table_field in tag.fields and
            table_lower in tag.fields[table_field].string_value.lower()
        ]

    @classmethod
    def __filter_column_matching_tags(
            cls,
            tags: List[Tag],
            column: Optional[str] = None
    ) -> List[Tag]:  # yapf: disable
        """Filter Tags that have a `column` field that match the provided
        column name.

        Returns:
            list: The filtered Tags.
        """
        if not column:
            return []

        column_field = 'column'
        column_lower = column.lower()

        return [
            tag for tag in tags or [] if column_field in tag.fields and
            column_lower in tag.fields[column_field].string_value.lower()
        ]
