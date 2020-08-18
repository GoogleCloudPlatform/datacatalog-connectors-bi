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

from google.cloud.datacatalog import types

from google.datacatalog_connectors.commons import prepare


class DataCatalogTagFactory(prepare.BaseTagFactory):
    __STRING_VALUE_ARRAY_ELEM_SEP = ', '

    # String field values are limited to 2000 bytes size when encoded in UTF-8.
    __STRING_VALUE_MAX_LENGTH = 2000

    def __init__(self, instance_url):
        self.__instance_url = instance_url

    def make_tag_for_dashboard(self, tag_template, dashboard):
        tag = types.Tag()

        tag.template = tag_template.name

        self._set_string_field(tag, 'id', dashboard.id)
        self._set_string_field(tag, 'description', dashboard.description)
        self._set_string_field(tag, 'folder_id', dashboard.space.id)
        self._set_string_field(tag, 'folder_name', dashboard.space.name)
        self._set_bool_field(tag, 'is_hidden', dashboard.hidden)
        self._set_double_field(tag, 'user_id', dashboard.user_id)
        self._set_double_field(tag, 'view_count', dashboard.view_count)
        self._set_double_field(tag, 'favorite_count', dashboard.favorite_count)
        self._set_timestamp_field(tag, 'last_accessed_at',
                                  dashboard.last_accessed_at)
        self._set_timestamp_field(tag, 'last_viewed_at',
                                  dashboard.last_viewed_at)
        self._set_bool_field(tag, 'is_deleted', dashboard.deleted)
        self._set_timestamp_field(tag, 'deleted_at', dashboard.deleted_at)
        self._set_double_field(tag, 'deleter_id', dashboard.deleter_id)

        self._set_string_field(tag, 'instance_url', self.__instance_url)

        return tag

    def make_tag_for_dashboard_element(self, tag_template, element, dashboard):
        tag = types.Tag()

        tag.template = tag_template.name

        self._set_string_field(tag, 'id', element.id)
        self._set_string_field(tag, 'type', element.type)
        self._set_string_field(tag, 'dashboard_id', element.dashboard_id)

        if dashboard:
            self._set_string_field(tag, 'dashboard_title', dashboard.title)

        self._set_string_field(tag, 'look_id', element.look_id)

        if element.look:
            self._set_string_field(tag, 'look_title', element.look.title)

        self._set_string_field(tag, 'lookml_link_id', element.lookml_link_id)

        if element.query_id:
            self._set_double_field(tag, 'query_id', element.query_id)
        elif element.result_maker:
            self._set_double_field(tag, 'query_id',
                                   element.result_maker.query_id)

        self._set_string_field(tag, 'instance_url', self.__instance_url)

        return tag

    def make_tag_for_folder(self, tag_template, folder):
        tag = types.Tag()

        tag.template = tag_template.name

        self._set_string_field(tag, 'id', folder.id)
        self._set_string_field(tag, 'name', folder.name)
        self._set_string_field(tag, 'parent_id', folder.parent_id)
        self._set_bool_field(tag, 'has_children', folder.child_count > 0)
        self._set_double_field(tag, 'children_count', folder.child_count)

        has_dashboards = hasattr(folder, 'dashboards')
        if has_dashboards:
            has_dashboards = True if folder.dashboards else False
        self._set_bool_field(tag, 'has_dashboards', has_dashboards)
        dashboards_count = len(folder.dashboards) if has_dashboards else 0
        self._set_double_field(tag, 'dashboards_count', dashboards_count)

        has_looks = hasattr(folder, 'looks')
        if has_looks:
            has_looks = True if folder.looks else False
        self._set_bool_field(tag, 'has_looks', has_looks)
        looks_count = len(folder.looks) if has_looks else 0
        self._set_double_field(tag, 'looks_count', looks_count)

        self._set_string_field(tag, 'instance_url', self.__instance_url)

        return tag

    def make_tag_for_look(self, tag_template, look):
        tag = types.Tag()

        tag.template = tag_template.name

        self._set_double_field(tag, 'id', look.id)
        self._set_string_field(tag, 'description', look.description)
        self._set_string_field(tag, 'folder_id', look.space.id)
        self._set_string_field(tag, 'folder_name', look.space.name)
        self._set_bool_field(tag, 'is_public', look.public)
        self._set_double_field(tag, 'user_id', look.user_id)
        self._set_double_field(tag, 'last_updater_id', look.last_updater_id)
        self._set_double_field(tag, 'query_id', look.query_id)

        # Some objects, such as LookWithDashboards instances, don't have
        # the url attribute.
        if hasattr(look, 'url'):
            self._set_string_field(tag, 'url', look.url)

        self._set_string_field(tag, 'short_url', look.short_url)
        self._set_string_field(tag, 'public_url', look.public_url)
        self._set_string_field(tag, 'excel_file_url', look.excel_file_url)
        self._set_string_field(tag, 'google_spreadsheet_formula',
                               look.google_spreadsheet_formula)
        self._set_double_field(tag, 'view_count', look.view_count)
        self._set_double_field(tag, 'favorite_count', look.favorite_count)
        self._set_timestamp_field(tag, 'last_accessed_at',
                                  look.last_accessed_at)
        self._set_timestamp_field(tag, 'last_viewed_at', look.last_viewed_at)
        self._set_bool_field(tag, 'is_deleted', look.deleted)
        self._set_timestamp_field(tag, 'deleted_at', look.deleted_at)
        self._set_double_field(tag, 'deleter_id', look.deleter_id)

        self._set_string_field(tag, 'instance_url', self.__instance_url)

        return tag

    def make_tag_for_query(self, tag_template, assembled_query_metadata):
        tag = types.Tag()

        tag.template = tag_template.name

        query = assembled_query_metadata.query
        generated_sql = assembled_query_metadata.generated_sql
        model_explore = assembled_query_metadata.model_explore
        connection = assembled_query_metadata.connection

        self._set_double_field(tag, 'id', query.id)

        if query.fields:
            value = self.__STRING_VALUE_ARRAY_ELEM_SEP.join(query.fields)
            self._set_string_field(tag, 'fields', value)

        if query.pivots:
            value = self.__STRING_VALUE_ARRAY_ELEM_SEP.join(query.pivots)
            self._set_string_field(tag, 'pivots', value)

        if query.sorts:
            value = self.__STRING_VALUE_ARRAY_ELEM_SEP.join(query.sorts)
            self._set_string_field(tag, 'sorts', value)

        self._set_double_field(tag, 'runtime', query.runtime)
        self._set_string_field(tag, 'client_id', query.client_id)
        self._set_string_field(tag, 'query_timezone', query.query_timezone)
        self._set_string_field(tag, 'lookml_model', query.model)
        self._set_string_field(tag, 'explore_name', query.view)

        self._set_string_field(tag, 'sql', generated_sql)

        if model_explore:
            self._set_string_field(tag, 'lookml_project',
                                   model_explore.project_name)
            self._set_string_field(tag, 'connection',
                                   model_explore.connection_name)

        if connection:
            self._set_string_field(tag, 'host', connection.host)
            self._set_string_field(tag, 'database', connection.database)
            self._set_string_field(tag, 'connection_dialect',
                                   connection.dialect_name)
            self._set_string_field(tag, 'connection_username',
                                   connection.username)

        self._set_string_field(tag, 'instance_url', self.__instance_url)

        return tag
