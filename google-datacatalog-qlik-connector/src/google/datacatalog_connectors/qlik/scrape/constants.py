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

import random
import string

JSON_CONTENT_TYPE = 'application/json'

# The Qlik Sense Proxy Service (QPS) manages the Qlik Sense authentication,
# session handling, and load balancing. This constant is used to store the name
# of the cookie issue to a user after authenticating in QPS.
QPS_SESSION_COOKIE_PREFIX = 'X-Qlik-Session'

WINDOWS_USER_AGENT = 'Windows'

# XRFKEY is an arbitrary 16-char-length string composed of letters and digits.
# The below code generates a unique string per connector execution just avoid
# hardcoded values.
XRFKEY = ''.join(random.choices(string.ascii_letters + string.digits, k=16))
XRFKEY_HEADER_NAME = 'x-Qlik-Xrfkey'
