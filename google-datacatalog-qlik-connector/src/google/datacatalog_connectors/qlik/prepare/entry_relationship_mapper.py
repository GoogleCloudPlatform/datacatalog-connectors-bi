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

from google.datacatalog_connectors.commons import prepare

from google.datacatalog_connectors.qlik.prepare import constants


class EntryRelationshipMapper(prepare.BaseEntryRelationshipMapper):
    __APP = constants.USER_SPECIFIED_TYPE_APP
    __CUSTOM_PROPERTY_DEFINITION = \
        constants.USER_SPECIFIED_TYPE_CUSTOM_PROPERTY_DEFINITION
    __DIMENSION = constants.USER_SPECIFIED_TYPE_DIMENSION
    __MEASURE = constants.USER_SPECIFIED_TYPE_MEASURE
    __VISUALIZATION = constants.USER_SPECIFIED_TYPE_VISUALIZATION
    __SHEET = constants.USER_SPECIFIED_TYPE_SHEET
    __STREAM = constants.USER_SPECIFIED_TYPE_STREAM

    def fulfill_tag_fields(self, assembled_entries):
        resolvers = (
            self.__resolve_app_mappings,
            self.__resolve_dimension_mappings,
            self.__resolve_measure_mappings,
            self.__resolve_sheet_mappings,
            self.__resolve_stream_mappings,
            self.__resolve_visualization_mappings,
        )

        self._fulfill_tag_fields(assembled_entries, resolvers)

    @classmethod
    def __resolve_app_mappings(cls, assembled_entries, id_name_pairs):
        app_assembled_entries = [
            assembled_entry for assembled_entry in assembled_entries
            if assembled_entry.entry.user_specified_type == cls.__APP
        ]
        for assembled_entry in app_assembled_entries:
            cls._map_related_entry(assembled_entry,
                                   cls.__CUSTOM_PROPERTY_DEFINITION,
                                   'property_definition_id',
                                   'property_definition_entry', id_name_pairs)
            cls._map_related_entry(assembled_entry, cls.__STREAM, 'stream_id',
                                   'stream_entry', id_name_pairs)

    @classmethod
    def __resolve_dimension_mappings(cls, assembled_entries, id_name_pairs):
        dimension_assembled_entries = [
            assembled_entry for assembled_entry in assembled_entries
            if assembled_entry.entry.user_specified_type == cls.__DIMENSION
        ]
        for assembled_entry in dimension_assembled_entries:
            cls._map_related_entry(assembled_entry, cls.__APP, 'app_id',
                                   'app_entry', id_name_pairs)

    @classmethod
    def __resolve_measure_mappings(cls, assembled_entries, id_name_pairs):
        measure_assembled_entries = [
            assembled_entry for assembled_entry in assembled_entries
            if assembled_entry.entry.user_specified_type == cls.__MEASURE
        ]
        for assembled_entry in measure_assembled_entries:
            cls._map_related_entry(assembled_entry, cls.__APP, 'app_id',
                                   'app_entry', id_name_pairs)

    @classmethod
    def __resolve_sheet_mappings(cls, assembled_entries, id_name_pairs):
        sheet_assembled_entries = [
            assembled_entry for assembled_entry in assembled_entries
            if assembled_entry.entry.user_specified_type == cls.__SHEET
        ]
        for assembled_entry in sheet_assembled_entries:
            cls._map_related_entry(assembled_entry, cls.__APP, 'app_id',
                                   'app_entry', id_name_pairs)

    @classmethod
    def __resolve_stream_mappings(cls, assembled_entries, id_name_pairs):
        stream_assembled_entries = [
            assembled_entry for assembled_entry in assembled_entries
            if assembled_entry.entry.user_specified_type == cls.__STREAM
        ]
        for assembled_entry in stream_assembled_entries:
            cls._map_related_entry(assembled_entry,
                                   cls.__CUSTOM_PROPERTY_DEFINITION,
                                   'property_definition_id',
                                   'property_definition_entry', id_name_pairs)

    @classmethod
    def __resolve_visualization_mappings(cls, assembled_entries,
                                         id_name_pairs):

        viz_assembled_entries = [
            assembled_entry for assembled_entry in assembled_entries
            if assembled_entry.entry.user_specified_type == cls.__VISUALIZATION
        ]
        for assembled_entry in viz_assembled_entries:
            cls._map_related_entry(assembled_entry, cls.__APP, 'app_id',
                                   'app_entry', id_name_pairs)
