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

from google.datacatalog_connectors.commons import cleanup, ingest

from google.datacatalog_connectors.qlik import prepare, scrape
from google.datacatalog_connectors.qlik.prepare import constants


class MetadataSynchronizer:
    __ENTRY_GROUP_ID = 'qlik'
    __SPECIFIED_SYSTEM = 'qlik'
    __TAG_TEMPLATE_NAME_PATTERN = r'^(.+?)/tagTemplates/(?P<id>.+?)$'

    def __init__(self, qlik_server_address, qlik_ad_domain, qlik_username,
                 qlik_password, datacatalog_project_id,
                 datacatalog_location_id):

        self.__project_id = datacatalog_project_id
        self.__location_id = datacatalog_location_id

        self.__metadata_scraper = scrape.MetadataScraper(
            qlik_server_address, qlik_ad_domain, qlik_username, qlik_password)

        self.__tag_template_factory = prepare.DataCatalogTagTemplateFactory(
            project_id=datacatalog_project_id,
            location_id=datacatalog_location_id)

        self.__site_url = qlik_server_address

        self.__assembled_entry_factory = prepare.AssembledEntryFactory(
            project_id=datacatalog_project_id,
            location_id=datacatalog_location_id,
            entry_group_id=self.__ENTRY_GROUP_ID,
            user_specified_system=self.__SPECIFIED_SYSTEM,
            site_url=self.__site_url)

    def run(self):
        """Coordinates a full scrape > prepare > ingest process."""

        # Scrape metadata from the Qlik server.
        logging.info('')
        logging.info('===> Scraping Qlik Sense metadata...')

        logging.info('')
        logging.info('Objects to be scraped: Custom Property Definitions')
        custom_property_defs = self.__scrape_custom_property_definitions()

        logging.info('')
        logging.info('Objects to be scraped:'
                     ' Streams, Apps, Dimensions, Measures, and Sheets')
        streams = self.__scrape_streams()
        logging.info('==== DONE ========================================')

        # Prepare: convert Qlik metadata into Data Catalog entities model.
        logging.info('')
        logging.info('===> Converting Qlik metadata'
                     ' into Data Catalog entities model...')

        tag_templates_dict = self.__make_tag_templates_dict(
            custom_property_defs)

        assembled_entries_dict = self.__make_assembled_entries_dict(
            custom_property_defs, streams, tag_templates_dict)
        logging.info('==== DONE ========================================')

        # Data Catalog entry relationships mapping.
        logging.info('')
        logging.info('===> Mapping Data Catalog entry relationships...')

        self.__map_datacatalog_relationships(assembled_entries_dict)
        logging.info('==== DONE ========================================')

        # Data Catalog clean up: delete obsolete data.
        logging.info('')
        logging.info('===> Deleting Data Catalog obsolete metadata...')

        self.__delete_obsolete_entries(assembled_entries_dict)
        logging.info('==== DONE ========================================')

        # Ingest metadata into Data Catalog.
        logging.info('')
        logging.info('===> Synchronizing Qlik :: Data Catalog metadata...')

        self.__ingest_metadata(assembled_entries_dict, tag_templates_dict)
        logging.info('==== DONE ========================================')

    def __scrape_custom_property_definitions(self):
        """Scrapes metadata from all the Custom Property Definitions the
        current user has access to.

        :return: A ``list`` of Custom Property Definition metadata.
        """
        return self.__metadata_scraper.scrape_all_custom_property_definitions()

    def __scrape_streams(self):
        """Scrapes metadata from all the Streams the current user has access
        to.

        The returned metadata include nested objects such as Apps and
        Sheets.

        :return: A ``list`` of Stream metadata.
        """
        all_streams = self.__metadata_scraper.scrape_all_streams()
        all_apps = self.__metadata_scraper.scrape_all_apps()

        # Not being published means the app is a work in progress, so it can be
        # skipped.
        published_apps = [app for app in all_apps if app.get('published')]

        for app in published_apps:
            # The below fields are not available in the scrape apps API
            # response, so they are injected into the returned metadata object
            # to turn further processing more efficient.
            app['dimensions'] = self.__scrape_dimensions(app)
            app['measures'] = self.__scrape_measures(app)
            app['visualizations'] = self.__scrape_visualizations(app)
            app['sheets'] = self.__scrape_published_sheets(app)

        self.__assemble_streams_metadata_from_flat_lists(
            all_streams, published_apps)

        return all_streams

    def __scrape_dimensions(self, app):
        """Scrapes metadata from the Dimensions the current user has access to
        within the given App.

        :return: A ``list`` of dimension metadata.
        """
        dimensions = self.__metadata_scraper.scrape_dimensions(app)
        # The 'app' field is not available in the scrape dimensions API
        # response, so it is injected into the returned metadata object to turn
        # further processing more efficient.
        for dimension in dimensions:
            dimension['app'] = {
                'id': app.get('id'),
                'name': app.get('name'),
            }

        return dimensions

    def __scrape_measures(self, app):
        """Scrapes metadata from the Measures the current user has access to
        within the given App.

        :return: A ``list`` of measure metadata.
        """
        measures = self.__metadata_scraper.scrape_measures(app)
        # The 'app' field is not available in the scrape measures API
        # response, so it is injected into the returned metadata object to turn
        # further processing more efficient.
        for measure in measures:
            measure['app'] = {
                'id': app.get('id'),
                'name': app.get('name'),
            }

        return measures

    def __scrape_visualizations(self, app):
        """Scrapes metadata from the Visualizations the current user has access
        to within the given App.

        :return: A ``list`` of visualization metadata.
        """
        visualizations = self.__metadata_scraper.scrape_visualizations(app)
        # The 'app' field is not available in the scrape visualizations API
        # response, so it is injected into the returned metadata object to turn
        # further processing more efficient.
        for visualization in visualizations:
            visualization['app'] = {
                'id': app.get('id'),
                'name': app.get('name'),
            }

        return visualizations

    def __scrape_published_sheets(self, app):
        """Scrapes metadata from all the published Sheets the current user has
        access to within the given App.

        :return: A ``list`` of sheet metadata.
        """
        sheets = self.__metadata_scraper.scrape_sheets(app)
        # Not being published means the sheet is a work in progress, so it can
        # be skipped.
        published_sheets = [
            sheet for sheet in sheets if sheet.get('qMeta').get('published')
        ]
        # The 'app' field is not available in the scrape sheets API response,
        # so it is injected into the returned metadata object to turn further
        # processing more efficient.
        for sheet in published_sheets:
            sheet['app'] = {
                'id': app.get('id'),
                'name': app.get('name'),
            }

        return published_sheets

    @classmethod
    def __assemble_streams_metadata_from_flat_lists(cls, all_streams,
                                                    published_apps):
        """Assembles the streams metadata from the given flat asset lists.

        """
        streams_dict = {}
        # Build an id-based dict to promote faster lookup in the next steps.
        for stream in all_streams:
            streams_dict[stream.get('id')] = stream

        for app in published_apps:
            stream_id = app.get('stream').get('id')

            # The 'apps' field is not available in the scrape streams API
            # response, so it is injected into the returned metadata object to
            # turn further processing more efficient.
            if not streams_dict[stream_id].get('apps'):
                streams_dict[stream_id]['apps'] = []

            streams_dict[stream_id]['apps'].append(app)

        return all_streams

    def __make_tag_templates_dict(self, custom_property_defs):
        templates_dict = {}

        self.__add_template_to_dict(
            templates_dict,
            self.__tag_template_factory.make_tag_template_for_app())
        self.__add_template_to_dict(
            templates_dict,
            self.__tag_template_factory.
            make_tag_template_for_custom_property_definition())
        self.__add_template_to_dict(
            templates_dict,
            self.__tag_template_factory.make_tag_template_for_dimension())
        self.__add_template_to_dict(
            templates_dict,
            self.__tag_template_factory.make_tag_template_for_measure())
        self.__add_template_to_dict(
            templates_dict,
            self.__tag_template_factory.make_tag_template_for_visualization())
        self.__add_template_to_dict(
            templates_dict,
            self.__tag_template_factory.make_tag_template_for_sheet())
        self.__add_template_to_dict(
            templates_dict,
            self.__tag_template_factory.make_tag_template_for_stream())

        for definition in custom_property_defs:
            for value in definition.get('choiceValues') or []:
                self.__add_template_to_dict(
                    templates_dict,
                    self.__tag_template_factory.
                    make_tag_template_for_custom_property_value(
                        definition, value))

        return templates_dict

    def __add_template_to_dict(self, templates_dict, template):
        template_id = re.match(pattern=self.__TAG_TEMPLATE_NAME_PATTERN,
                               string=template.name).group('id')
        templates_dict[template_id] = template

    def __make_assembled_entries_dict(self, custom_property_defs_metadata,
                                      streams_metadata, tag_templates_dict):
        """Makes Data Catalog entries and tags for the Qlik assets the current
        user has access to.

        Returns:
            A ``dict`` in which keys are the top level asset ids and values are
            flat lists containing those assets and their nested ones, with all
            related entries and tags.
        """
        assembled_entries = {}

        property_def_tag_template = tag_templates_dict.get(
            constants.TAG_TEMPLATE_ID_CUSTOM_PROPERTY_DEFINITION)
        for property_def_metadata in custom_property_defs_metadata:
            assembled_entries[property_def_metadata.get('id')] = \
                [self.__assembled_entry_factory
                    .make_assembled_entry_for_custom_property_def(
                        property_def_metadata, property_def_tag_template)]

        for stream_metadata in streams_metadata:
            assembled_entries[stream_metadata.get('id')] = \
                self.__assembled_entry_factory\
                    .make_assembled_entries_for_stream(
                        stream_metadata, tag_templates_dict)

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
            self.__project_id, self.__location_id, self.__ENTRY_GROUP_ID). \
            delete_obsolete_metadata(
            all_assembled_entries,
            f'system={self.__SPECIFIED_SYSTEM}'
            f' tag:site_url:{self.__site_url}')

    def __ingest_metadata(self, assembled_entries_dict, tag_templates_dict):
        metadata_ingestor = ingest.DataCatalogMetadataIngestor(
            self.__project_id, self.__location_id, self.__ENTRY_GROUP_ID)

        entries_count = sum(
            len(entries) for entries in assembled_entries_dict.values())
        logging.info('==== %d entries to be synchronized!', entries_count)

        synced_entries_count = self.__ingest_custom_property_defs_metadata(
            assembled_entries_dict, tag_templates_dict, metadata_ingestor)
        synced_entries_count += self.__ingest_streams_metadata(
            assembled_entries_dict, tag_templates_dict, metadata_ingestor)

        logging.info('')
        logging.info('==== %d of %d entries successfully synchronized!',
                     synced_entries_count, entries_count)

    def __ingest_custom_property_defs_metadata(self, assembled_entries_dict,
                                               tag_templates_dict,
                                               metadata_ingestor):

        custom_property_defs_assembled_entries = []
        for assembled_entries in assembled_entries_dict.values():
            if assembled_entries[0].entry.user_specified_type == \
                    constants.USER_SPECIFIED_TYPE_CUSTOM_PROPERTY_DEFINITION:
                custom_property_defs_assembled_entries.extend(
                    assembled_entries)

        custom_property_defs_entries_count = \
            len(custom_property_defs_assembled_entries)

        logging.info('')
        logging.info(
            '==== %d Custom Property Definition entries to be ingested...',
            custom_property_defs_entries_count)

        if not custom_property_defs_assembled_entries:
            return 0

        required_templates_dict = self.__filter_required_tag_templates(
            custom_property_defs_assembled_entries, tag_templates_dict)
        metadata_ingestor.ingest_metadata(
            custom_property_defs_assembled_entries, required_templates_dict)

        return custom_property_defs_entries_count

    def __ingest_streams_metadata(self, assembled_entries_dict,
                                  tag_templates_dict, metadata_ingestor):

        stream_entries_dict = {}
        for stream_id, assembled_entries in assembled_entries_dict.items():
            if assembled_entries[0].entry.user_specified_type == \
                    constants.USER_SPECIFIED_TYPE_STREAM:
                stream_entries_dict[stream_id] = assembled_entries

        synced_entries_count = 0
        for stream_id, assembled_entries in stream_entries_dict.items():
            asset_entries_count = len(assembled_entries)

            logging.info('')
            logging.info(
                '==== The Stream identified by "%s" and its nested assets'
                ' comprise %d entries.', stream_id, asset_entries_count)

            required_templates_dict = self.__filter_required_tag_templates(
                assembled_entries, tag_templates_dict)
            metadata_ingestor.ingest_metadata(assembled_entries,
                                              required_templates_dict)
            synced_entries_count += asset_entries_count

        return synced_entries_count

    def __filter_required_tag_templates(self, assembled_entries,
                                        tag_templates_dict):
        """Filters the Tag Templates that are required to ingest the given
        Entries and their Tags.

        This utility method should be called before any call to
        DataCatalogMetadataIngestor.ingest_metadata(). The tag_templates_dict,
        containing a variety of Tag Templates created to support the whole sync
        workflow, is intended to be shared among several calls to
        ingest_metadata(). Each call ingests a subset of Entries and Tags, so
        the rationale here is to filter only the Tag Templates required for a
        specific metadata ingestion round. By doing this, it avoids unnecessary
        Tag Template processing, which includes billable API calls, during the
        ingestion process.

        :return: A ``dict``.
        """
        required_templates_dict = {}

        for assembled_entry in assembled_entries:
            for tag in assembled_entry.tags:
                template_id = re.match(
                    pattern=self.__TAG_TEMPLATE_NAME_PATTERN,
                    string=tag.template).group('id')
                required_templates_dict[template_id] = tag_templates_dict.get(
                    template_id)

        return required_templates_dict
