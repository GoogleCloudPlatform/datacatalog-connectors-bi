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
from typing import Dict, List, Optional, Union

from google.cloud.datacatalog import ColumnSchema, Entry, Tag
from google.datacatalog_connectors import commons

from google.datacatalog_connectors.sisense.prepare import constants


class ElastiCubeDependencyFinder:
    """Base class for features that rely on Google Data Catalog to find
    ElastiCube-related dependencies.
    """
    # Regex used to parse Tag Template names and get specific parts.
    __TAG_TEMPLATE_NAME_PATTERN = r'^(.+?)/tagTemplates/(?P<id>.+?)$'

    def __init__(self, project_id: Optional[str] = None):
        self.__datacatalog_facade = commons.DataCatalogFacade(project_id)

    @classmethod
    def _make_query(cls,
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

    def _search_catalog(self, query: str) -> List[str]:
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
                     len(entry_names) if entry_names else 'No')

        return entry_names

    def _get_entries(self, entry_names: List[str]) -> Dict[str, Entry]:
        """Get Data Catalog Entries for a given list of entry names.

        Returns:
            dict: Keys are the names and values their corresponding entries.
        """
        entries_dict = {}
        for entry_name in entry_names or []:
            entries_dict[entry_name] = self.__datacatalog_facade.get_entry(
                entry_name)

        return entries_dict

    def _list_tags(self, entry_names: List[str]) -> Dict[str, List[Tag]]:
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
    def _sort_tags_by_schema(
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
                cls._sort_tags_by_schema(column.subcolumns, f'{column_path}.',
                                         tags_dict))

        return sorted_tags
