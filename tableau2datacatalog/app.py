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

import os

import flask

# Imports the Google Cloud Logging client library
import google.cloud.logging

from google.datacatalog_connectors.tableau import sync

# Instantiates logging a client
client = google.cloud.logging.Client()

# Connects the logger to the root logging handler; by default this captures
# all logs at INFO level and higher
client.setup_logging()

app = flask.Flask(__name__)


@app.route('/', methods=['POST', 'GET'])
def run():
    valid_api_key = os.environ['TABLEAU2DC_API_KEY']

    received_api_key = flask.request.args.get('api_key')

    if valid_api_key != received_api_key:
        return flask.make_response(
            flask.jsonify({
                'message': 'Unauthorized Call!',
                'code': 'DENIED'
            }), 401)

    if flask.request.method == 'POST':
        request_data = flask.request.get_json()
        app.logger.info(request_data)

        # TODO improve this logic to pass other filter options to the
        #  Synchronizer, according to the hook event type.
        query_filters = {'workbooks': {'luid': request_data['resource_luid']}}

        sync.datacatalog_synchronizer.DataCatalogSynchronizer(
            tableau_server_address=os.environ['TABLEAU2DC_TABLEAU_SERVER'],
            tableau_api_version=os.environ['TABLEAU2DC_TABLEAU_API_VERSION'],
            tableau_username=os.environ['TABLEAU2DC_TABLEAU_USERNAME'],
            tableau_password=os.environ['TABLEAU2DC_TABLEAU_PASSWORD'],
            tableau_site=os.environ['TABLEAU2DC_TABLEAU_SITE'],
            datacatalog_project_id=os.
            environ['TABLEAU2DC_DATACATALOG_PROJECT_ID'],
            datacatalog_location_id=os.
            environ['TABLEAU2DC_DATACATALOG_LOCATION_ID']).run(
                query_filters=query_filters)

        response = {'message': 'Synchronized', 'code': 'SUCCESS'}
        return flask.make_response(flask.jsonify(response), 200)
    elif flask.request.method == 'GET':
        return """use POST method with a message event BODY"""


if __name__ == "__main__":
    app.run(debug=False,
            host='0.0.0.0',
            port=int(os.environ.get('PORT', 8080)))
