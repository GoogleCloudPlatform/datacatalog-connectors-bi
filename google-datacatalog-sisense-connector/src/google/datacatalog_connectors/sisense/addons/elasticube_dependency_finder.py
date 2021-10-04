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
import re
from typing import Dict, List, Optional, Tuple, Union

from google.cloud.datacatalog import ColumnSchema, Entry, Tag
from google.datacatalog_connectors import commons

from google.datacatalog_connectors.sisense.prepare import constants


class ElastiCubeDependencyFinder:
    """Count on Google Data Catalog search capabilities to find
    ElastiCube-related dependencies.

    The public methods are intended to provide clients a simplified interface
    for Data Catalog search and results handling.
    """
    # Regex used to parse Tag Template names and get specific parts.
    __TAG_TEMPLATE_NAME_PATTERN = r'^(.+?)/tagTemplates/(?P<id>.+?)$'

    def __init__(self, project_id: str):
        self.__datacatalog_facade = commons.DataCatalogFacade(project_id)

    def find(self,
             datasource: str = None,
             table: str = None,
             column: str = None) -> Dict[str, Tuple[Entry, List[Tag]]]:
        """
        Orchestrate actions that comprise:
            - generating a query string to search Google Data Catalog;
            - performing a catalog search;
            - getting Entries and Tags based on search results;
            - filtering Tags that contain Entry enrichment metadata or match
                the table/column search criteria;
            - assembling the Entries and Tags in a user-friendly dict.

        Returns:
            dict: Keys are Entry names and values are tuples in which the
            first element is an Entry object and the second is an array of
            ElastiCube-related Tags.
        """
        logging.info('')
        logging.info('===> Searching Data Catalog...')
        query = self.__make_query(datasource=datasource,
                                  table=table,
                                  column=column)
        search_results = self.__datacatalog_facade.search_catalog(query)
        logging.info('==== DONE ========================================')

        logging.info('')
        logging.info('Catalog search returned %d results', len(search_results))
        if not search_results:
            return {}

        entry_names = [
            result.relative_resource_name for result in search_results
        ]

        logging.info('')
        logging.info('===> Getting entries...')
        entries_dict = self.__get_entries(entry_names)
        logging.info('==== DONE ========================================')

        logging.info('')
        logging.info('===> Listing tags...')
        tags_dict = self.__list_tags(entry_names)
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

        return assembled_results

    @classmethod
    def __make_query(cls,
                     datasource: str = None,
                     table: str = None,
                     column: str = None) -> str:
        """Make a Data Catalog search string by joining static and dynamic
        terms.

        Static search qualifiers (always added to the resulting string):
            - system=<system>
            - type=<type>

        Dynamic search qualifiers (depend on user input to be added to the
        resulting string):
            - tag:datasource:val
            - tag:table:val
            - tag:column:val

        Returns:
            str: The search string.

        Raises:
            Exception: If no datasource, table, or column are provided.
        """
        if not (datasource or table or column):
            raise Exception(
                'Either a datasource, table, or column must be provided.')

        query_terms = [
            'system=Sisense',
            cls.__make_type_search_term([
                constants.USER_SPECIFIED_TYPE_DASHBOARD,
                constants.USER_SPECIFIED_TYPE_WIDGET
            ]),
            cls.__make_datasource_search_term(datasource) or '',
            cls.__make_table_search_term(table) or '',
            cls.__make_column_search_term(column) or ''
        ]
        query = ' '.join(query_terms)
        logging.info('Query: %s', query)
        return query

    @classmethod
    def __make_type_search_term(cls, types: List[str]) -> Optional[str]:
        """Make a Data Catalog search string with the `type` qualifier.

        Logical `OR` is used to join the types if multiple values are provided,
        which means all of them will be considered in subsequent catalog search
        operations.

        Returns:
            str: The search term.
        """
        if not types:
            return None

        type_query_terms = [f'type={type_str}' for type_str in types]
        return f'({" OR ".join(type_query_terms)})'

    @classmethod
    def __make_datasource_search_term(cls, datasource: str) -> Optional[str]:
        """Make a Data Catalog search string with the `tag:datasource:val`
        qualifier.

        Sisense assets that connect to ElastiCube data sources, such as
        Dashboards and Widgets, have the `datasource` field in their metadata
        enrichment Tags, so we can leverage it when looking for ElastiCube
        dependencies.

        Returns:
            str: The search term.
        """
        return f'tag:datasource:"{datasource}"' if datasource else None

    @classmethod
    def __make_table_search_term(cls, table: str) -> Optional[str]:
        """Make a Data Catalog search string with the `tag:table:val`
        qualifier.

        Sisense assets that use ElastiCube tables, such as Dashboard/Widget
        fields and filters, have the `table` field in their JAQL Tags, so we
        can leverage it when looking for ElastiCube dependencies.

        Returns:
            str: The search term.
        """
        return f'tag:table:"{table}"' if table else None

    @classmethod
    def __make_column_search_term(cls, column: str) -> Optional[str]:
        """Make a Data Catalog search string with the `tag:column:val`
        qualifier.

        Sisense assets that use ElastiCube columns, such as Dashboard/Widget
        fields and filters, have the `column` field in their JAQL Tags, so we
        can leverage it when looking for ElastiCube dependencies.

        Returns:
            str: The search term.
        """
        return f'tag:column:"{column}"' if column else None

    def __get_entries(self, entry_names: List[str]) -> Dict[str, Entry]:
        """Get Data Catalog Entries for a given list of entry names.

        Returns:
            dict: Keys are the names and values their corresponding entries.
        """
        entries_dict = {}
        for entry_name in entry_names or []:
            entries_dict[entry_name] = self.__datacatalog_facade.get_entry(
                entry_name)

        return entries_dict

    def __list_tags(self, entry_names: List[str]) -> Dict[str, List[Tag]]:
        """Get Data Catalog Tags for a given list of entry names.

        Returns:
            dict: Keys are the entry names and values are lists that contains
            all their related tags.
        """
        tags_dict = {}
        for entry_name in entry_names or []:
            tags_dict[entry_name] = self.__datacatalog_facade.list_tags(
                entry_name)

        return tags_dict

    @classmethod
    def __filter_relevant_tags(cls, entry: Entry, tags: List[Tag], table: str,
                               column: str) -> List[Tag]:
        """Filter Tags for ElastiCube dependencies finding. "Relevant", in this
        context, means Tags that have ElastiCube-related information such as
        data source, table, or column names.

        Returns:
            list: The filtered Tags.
        """
        filtered_tags = []

        asset_metadata_tag = cls.filter_asset_metadata_tag(tags)
        if asset_metadata_tag:
            filtered_tags.append(asset_metadata_tag)
        table_column_matching_tags = cls.__filter_table_column_matching_tags(
            table, column, tags)
        filtered_tags.extend(
            cls.__sort_tags_by_schema(entry.schema.columns, '',
                                      table_column_matching_tags))

        return filtered_tags

    @classmethod
    def filter_asset_metadata_tag(cls, tags: List[Tag]) -> Optional[Tag]:
        """Filter Dashboard and Widget metadata enrichment Tags created from
        Templates identified by the `TAG_TEMPLATE_ID_DASHBOARD` and
        `TAG_TEMPLATE_ID_WIDGET` constants.

        Returns:
            list: The filtered Tags.
        """
        template_ids = [
            constants.TAG_TEMPLATE_ID_DASHBOARD,
            constants.TAG_TEMPLATE_ID_WIDGET
        ]

        for tag in tags or []:
            template_id = re.match(pattern=cls.__TAG_TEMPLATE_NAME_PATTERN,
                                   string=tag.template).group('id')

            if template_id in template_ids:
                return tag

    @classmethod
    def __filter_table_column_matching_tags(cls, table: str, column: str,
                                            tags: List[Tag]) -> List[Tag]:
        """Filter JAQL-related Tags which `table` and/or `column` fields match
        the provided values.

        If both `table` and `column` args are provided, a given Tag's
        corresponding fields must match both values in order to added to the
        resulting list.

        Returns:
            list: The filtered Tags.
        """
        jaql_tags = cls.filter_jaql_tags(tags)
        table_matching_tags = cls.__filter_table_matching_tags(
            table, jaql_tags)
        column_matching_tags = cls.__filter_column_matching_tags(
            column, table_matching_tags if table and column else tags)

        return column_matching_tags if column else table_matching_tags

    @classmethod
    def filter_jaql_tags(cls, tags: List[Tag]) -> List[Tag]:
        """Filter JAQL-related Tags created from a Template identified by the
        `TAG_TEMPLATE_ID_JAQL` constant.

        Returns:
            list: The filtered Tags.
        """
        template_ids = [constants.TAG_TEMPLATE_ID_JAQL]

        filtered_tags = []
        for tag in tags or []:
            template_id = re.match(pattern=cls.__TAG_TEMPLATE_NAME_PATTERN,
                                   string=tag.template).group('id')

            if template_id in template_ids:
                filtered_tags.append(tag)

        return filtered_tags

    @classmethod
    def __filter_table_matching_tags(cls, table: str,
                                     tags: List[Tag]) -> List[Tag]:
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
    def __filter_column_matching_tags(cls, column: str,
                                      tags: List[Tag]) -> List[Tag]:
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

    @classmethod
    def __sort_tags_by_schema(
            cls, schema_columns: List[ColumnSchema], schema_path: str,
            tags: Union[List[Tag], Dict[str, Tag]]) -> List[Tag]:
        """Sort column-level Tags according to the order their related columns
        are listed in the parent Entry's schema.

        This method is intended to bring user friendliness to search results
        processing as it ensures column-level Tags will be sorted exactly as
        they are in the Data Catalog UI.

        Returns:
            list: The sorted Tags.
        """
        sorted_tags = []

        if not schema_columns:
            return sorted_tags

        # No matter whether the `tags` arg is a `list` or a `dict` with unknown
        # keys, this block converts into a `dict` in which keys are the Tags'
        # column names and values are the Tag objects. Only column-level Tags
        # are added to the resulting `dict`.
        tags_dict = tags if isinstance(tags, dict) else None
        if isinstance(tags, list):
            tags_dict = {
                tag.column.lower(): tag for tag in tags or [] if tag.column
            }

        for column in schema_columns:
            column_path = f'{schema_path}{column.column}'.lower()
            column_tag = tags_dict.get(column_path)
            if column_tag:
                sorted_tags.append(column_tag)
            sorted_tags.extend(
                cls.__sort_tags_by_schema(column.subcolumns, f'{column_path}.',
                                          tags_dict))

        return sorted_tags
