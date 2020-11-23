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

import logging

from google.datacatalog_connectors.commons import cleanup, ingest

from google.datacatalog_connectors.qlik import prepare, scrape
from google.datacatalog_connectors.qlik.prepare import constants


class MetadataSynchronizer:
    __ENTRY_GROUP_ID = 'qlik'
    __SPECIFIED_SYSTEM = 'qlik'

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
        logging.info('Streams and apps...')
        streams = self.__scrape_streams()
        logging.info('==== DONE ========================================')

        # Prepare: convert Qlik metadata into Data Catalog entities model.
        logging.info('')
        logging.info('===> Converting Qlik metadata'
                     ' into Data Catalog entities model...')

        tag_templates_dict = self.__make_tag_templates_dict()

        assembled_entries_dict = self.__make_assembled_entries_dict(
            streams, tag_templates_dict)
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

        self.__ingest_metadata(tag_templates_dict, assembled_entries_dict)
        logging.info('==== DONE ========================================')

    def __scrape_streams(self):
        """Scrape metadata from all streams the current user has access to.
        Streams metadata include nested objects such as apps.

        :return: A ``list`` of stream metadata.
        """
        all_streams = self.__metadata_scraper.scrape_all_streams()
        all_apps = self.__metadata_scraper.scrape_all_apps()

        self.__assemble_stream_metadata_from_flat_lists(all_streams, all_apps)

        return all_streams

    @classmethod
    def __assemble_stream_metadata_from_flat_lists(cls, all_streams, all_apps):
        """Assemble the stream's metadata from the given flat asset lists.

        """
        streams_dict = {}
        # Build an id-based dict to promote faster lookup in the next steps.
        for stream in all_streams:
            streams_dict[stream.get('id')] = stream

        for app in all_apps:
            # Not having a stream means the app is a work in progress and has
            # not been published, so it can be skipped.
            if not app.get('stream'):
                continue

            stream_id = app.get('stream').get('id')

            # The 'apps' field is not available in the API response but is
            # injected into the returned metadata object to make further
            # processing more efficient.
            if not streams_dict[stream_id].get('apps'):
                streams_dict[stream_id]['apps'] = []

            streams_dict[stream_id]['apps'].append(app)

        return all_streams

    def __make_tag_templates_dict(self):
        return {
            constants.TAG_TEMPLATE_ID_APP:
                self.__tag_template_factory.make_tag_template_for_app(),
            constants.TAG_TEMPLATE_ID_STREAM:
                self.__tag_template_factory.make_tag_template_for_stream(),
        }

    def __make_assembled_entries_dict(self, streams_metadata,
                                      tag_templates_dict):
        """Make Data Catalog entries and tags for the Qlik assets the current
        user has access to.

        Returns:
            A ``dict`` in which keys are equals to the stream keys and values
            are flat lists containing assembled objects with all their related
            entries and tags.
        """
        assembled_entries = {}

        for stream_metadata in streams_metadata:
            assembled_entries[stream_metadata.get('id')] = \
                self.__assembled_entry_factory.make_assembled_entries_list(
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

    def __ingest_metadata(self, tag_templates_dict, assembled_entries_dict):
        metadata_ingestor = ingest.DataCatalogMetadataIngestor(
            self.__project_id, self.__location_id, self.__ENTRY_GROUP_ID)

        entries_count = sum(
            len(entries) for entries in assembled_entries_dict.values())
        logging.info('==== %d entries to be synchronized!', entries_count)

        synced_entries_count = 0
        for stream_id, assembled_entries in assembled_entries_dict.items():
            stream_entries_count = len(assembled_entries)

            logging.info('')
            logging.info('==== The Stream identified by %s has %d entries.',
                         stream_id, stream_entries_count)
            metadata_ingestor.ingest_metadata(assembled_entries,
                                              tag_templates_dict)
            synced_entries_count = synced_entries_count + stream_entries_count

        logging.info('')
        logging.info('==== %d of %d entries successfully synchronized!',
                     synced_entries_count, entries_count)
