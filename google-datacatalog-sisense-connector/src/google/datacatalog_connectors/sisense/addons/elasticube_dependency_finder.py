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

    def __init__(self, project_id: Optional[str] = None):
        self.__datacatalog_facade = commons.DataCatalogFacade(project_id)

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
        query = self.__make_query(types,
                                  datasource=datasource,
                                  table=table,
                                  column=column)
        entry_names = self.__search_catalog(query)

        if not entry_names:
            return {}

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
                                             self.__filter_find_relevant_tags(
                                                 entry, tags, table, column))
        logging.info('==== DONE ========================================')

        logging.info('')
        logging.info('%d assembled result(s) to be returned',
                     len(assembled_results))
        logging.info('')

        return assembled_results

    def list_all(self,
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
        query = self.__make_query(types, asset_id=asset_id)
        tmp_entry_names = self.__search_catalog(query)

        if not tmp_entry_names:
            logging.info('')
            logging.info('Not found!')
            return {}

        logging.info('')
        logging.info('===> Getting entries...')
        tmp_entries_dict = self.__get_entries(tmp_entry_names)
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
        tags_dict = self.__list_tags(entry_names)
        logging.info('==== DONE ========================================')

        logging.info('')
        logging.info('===> Assembling results...')
        assembled_results = {}
        for entry_name in entry_names:
            entry = entries_dict.get(entry_name)
            tags = tags_dict.get(entry_name)
            assembled_results[entry_name] = (entry,
                                             self.__filter_list_relevant_tags(
                                                 entry, tags))
        logging.info('==== DONE ========================================')

        return assembled_results

    @classmethod
    def __make_query(cls,
                     types: List[str],
                     asset_id: Optional[str] = None,
                     dashboard_id: Optional[str] = None,
                     datasource: Optional[str] = None,
                     table: Optional[str] = None,
                     column: Optional[str] = None) -> str:
        """Make a Data Catalog search string by joining mandatory and optional
        terms.

        Mandatory search qualifiers (always added to the resulting string):
            - ``system=<system>``
            - ``type=<type>``

        Optional search qualifiers (depend on user input to be added to the
        resulting string):
            - ``tag:id:<val>``
            - ``tag:dashboard_id:<val>``
            - ``tag:datasource:<val>``
            - ``tag:table:<val>``
            - ``tag:column:<val>``

        Returns:
            str: The search string.
        """

        query_terms = [
            'system=Sisense',
            cls.__make_type_search_term(types),
            cls.__make_asset_id_search_term(asset_id) or '',
            cls.__make_dashboard_id_search_term(dashboard_id) or '',
            cls.__make_datasource_search_term(datasource) or '',
            cls.__make_table_search_term(table) or '',
            cls.__make_column_search_term(column) or ''
        ]
        return ' '.join(query_terms).strip()

    @classmethod
    def __make_type_search_term(cls, types: List[str]) -> Optional[str]:
        """Make a Data Catalog search string with the ``type=<type>``
        qualifier.

        Logical ``OR`` is used to join the types if multiple values are
        provided, which means all of them will be considered in subsequent
        catalog search operations.

        Returns:
            str: The search term.
        """
        if not types:
            return None

        type_query_terms = [f'type={type_str}' for type_str in types]
        return f'({" OR ".join(type_query_terms)})'

    @classmethod
    def __make_asset_id_search_term(
            cls, asset_id: Optional[str] = None
    ) -> Optional[str]:  # yapf: disable
        """Make a Data Catalog search string with the ``tag:id:<val>``
        qualifier.

        Dashboard and Widget related entries have an ``id`` field in their
        metadata enrichment Tags, so we can leverage it when looking for assets
        using their unique identifiers.

        Returns:
            str: The search term.
        """
        return f'tag:id:{asset_id}' if asset_id else None

    @classmethod
    def __make_dashboard_id_search_term(
            cls, dashboard_id: Optional[str] = None
    ) -> Optional[str]:  # yapf: disable
        """Make a Data Catalog search string with the
        ``tag:dashboard_id:<val>`` qualifier.

        Widget-related entries have a ``dashboard_id`` field in their metadata
        enrichment Tags, so we can leverage it when looking for the Widgets
        that compose a given Dashboard.

        Returns:
            str: The search term.
        """
        return f'tag:dashboard_id:{dashboard_id}' if dashboard_id else None

    @classmethod
    def __make_datasource_search_term(
            cls, datasource: Optional[str] = None
    ) -> Optional[str]:  # yapf: disable
        """Make a Data Catalog search string with the ``tag:datasource:<val>``
        qualifier.

        Sisense assets that connect to ElastiCube data sources, such as
        Dashboards and Widgets, have the ``datasource`` field in their metadata
        enrichment Tags, so we can leverage it when looking for ElastiCube
        dependencies.

        Returns:
            str: The search term.
        """
        return f'tag:datasource:"{datasource}"' if datasource else None

    @classmethod
    def __make_table_search_term(cls,
                                 table: Optional[str] = None) -> Optional[str]:
        """Make a Data Catalog search string with the ``tag:table:<val>``
        qualifier.

        Sisense assets that use ElastiCube tables, such as Dashboard/Widget
        fields and filters, have the ``table`` field in their JAQL Tags, so we
        can leverage it when looking for ElastiCube dependencies.

        Returns:
            str: The search term.
        """
        return f'tag:table:"{table}"' if table else None

    @classmethod
    def __make_column_search_term(
            cls, column: Optional[str] = None
    ) -> Optional[str]:  # yapf: disable
        """Make a Data Catalog search string with the ``tag:column:<val>``
        qualifier.

        Sisense assets that use ElastiCube columns, such as Dashboard/Widget
        fields and filters, have the ``column`` field in their JAQL Tags, so we
        can leverage it when looking for ElastiCube dependencies.

        Returns:
            str: The search term.
        """
        return f'tag:column:"{column}"' if column else None

    def __search_catalog(self, query: str) -> List[str]:
        """Perform a catalog search.

        Returns:
            list: The entry names.
        """
        logging.info('')
        logging.info('===> Searching Data Catalog...')
        logging.info('Query: %s', query)
        entry_names = self.__datacatalog_facade\
            .search_catalog_relative_resource_name(query)
        logging.info('==== DONE ========================================')
        logging.info('     %s results found',
                     len(entry_names)) if entry_names else 'No'

        return entry_names

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
    def __filter_find_relevant_tags(cls,
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
            cls.__sort_tags_by_schema(entry.schema.columns, '',
                                      table_column_matching_tags))

        return filtered_tags

    @classmethod
    def __filter_list_relevant_tags(cls, entry: Entry,
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
            cls.__sort_tags_by_schema(entry.schema.columns, '', jaql_tags))

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

        # No matter whether the `tags` arg is a `list` or a `dict`, this block
        # converts into a `dict` in which keys are the Tags' column names and
        # values are the Tag objects. Only column-level Tags are added to the
        # resulting `dict`.
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
