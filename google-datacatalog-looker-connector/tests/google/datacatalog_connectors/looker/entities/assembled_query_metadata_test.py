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

import unittest

from google.datacatalog_connectors.looker import entities


class AssembledQueryMetadataTest(unittest.TestCase):

    def test_constructor_should_set_instance_attributes(self):
        assembled_metadata = \
            entities.AssembledQueryMetadata({}, 'select *', {}, {})

        self.assertIsNotNone(assembled_metadata.__dict__['query'])
        self.assertEqual('select *',
                         assembled_metadata.__dict__['generated_sql'])
        self.assertIsNotNone(assembled_metadata.__dict__['model_explore'])
        self.assertIsNotNone(assembled_metadata.__dict__['connection'])
