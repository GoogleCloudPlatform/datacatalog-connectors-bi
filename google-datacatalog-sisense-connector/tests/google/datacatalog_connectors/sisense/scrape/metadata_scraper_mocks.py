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

from typing import Any, Dict, List, Union


class FakeResponse:

    def __init__(self,
                 json_data: Union[Dict[str, Any], List[Dict[str, Any]]],
                 status_code=200):

        self.__json_data = json_data
        self.status_code = status_code

    def json(self) -> Any:
        return self.__json_data
