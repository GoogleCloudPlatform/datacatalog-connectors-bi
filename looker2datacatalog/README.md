# looker2datacatalog

Package for ingesting Looker metadata into Google Cloud Data Catalog, currently
supporting below asset types:
- Folder
- Look
- Dashboard
- Dashboard Element (aka Tile)
- Query

**Disclaimer: This is not an officially supported Google product.**

<!--
  ⚠️ DO NOT UPDATE THE TABLE OF CONTENTS MANUALLY ️️⚠️
  run `npx markdown-toc -i README.md`.

  Please stick to 80-character line wraps as much as you can.
-->

## Table of Contents

<!-- toc -->

- [1. Environment setup](#1-environment-setup)
  * [1.1. Get the code](#11-get-the-code)
  * [1.2. Auth credentials](#12-auth-credentials)
      - [1.2.1. Create a GCP Service Account and grant it below roles](#121-create-a-gcp-service-account-and-grant-it-below-roles)
      - [1.2.2. Download a JSON key and save it as](#122-download-a-json-key-and-save-it-as)
      - [1.2.3. Create Looker API3 credentials](#123-create-looker-api3-credentials)
      - [1.2.4. Create a Looker configuration file](#124-create-a-looker-configuration-file)
  * [1.3. Virtualenv](#13-virtualenv)
      - [1.3.1. Install Python 3.7+](#131-install-python-37)
      - [1.3.2. Create and activate a *virtualenv*](#132-create-and-activate-a-virtualenv)
      - [1.3.3. Install the dependencies](#133-install-the-dependencies)
      - [1.3.4. Set environment variables](#134-set-environment-variables)
  * [1.4. Docker](#14-docker)
- [2. Sample application entry point](#2-sample-application-entry-point)
  * [2.1. Run the looker2datacatalog script](#21-run-the-looker2datacatalog-script)
- [3. Troubleshooting](#3-troubleshooting)

<!-- tocstop -->

-----

## 1. Environment setup

### 1.1. Get the code

````bash
git clone https://github.com/GoogleCloudPlatform/datacatalog-connectors-bi.git
cd datacatalog-connectors-bi/looker2datacatalog
````

### 1.2. Auth credentials

##### 1.2.1. Create a GCP Service Account and grant it below roles

- Data Catalog Admin

##### 1.2.2. Download a JSON key and save it as
- `<YOUR-CREDENTIALS_FILES_FOLDER>/looker2dc-datacatalog-credentials.json`

##### 1.2.3. Create Looker API3 credentials

The credentials required for API access must be obtained by creating an
API3 key on a user account in the Looker Admin console. The API3 key consists
of a public `client_id` and a private `client_secret`.

The shortcut for Looker Admin console is
https://<YOUR-LOOKER-ENDPOINT>/admin/users/api3_key/<YOUR-USER-ID>

##### 1.2.4. Create a Looker configuration file

File content is described in [Looker SDK documentation][1].
Save the file as
`<YOUR-CREDENTIALS_FILES_FOLDER>/looker2dc-looker-credentials.ini`


> Please notice this folder and files will be required in next steps.

### 1.3. Virtualenv

Using *virtualenv* is optional, but strongly recommended unless you use Docker
or a PEX file.

##### 1.3.1. Install Python 3.7+

##### 1.3.2. Create and activate a *virtualenv*

```bash
pip install --upgrade virtualenv
python3 -m virtualenv --python python3 env
source ./env/bin/activate
```

##### 1.3.3. Install the dependencies

```bash
pip install ./lib/datacatalog_connectors_commons-1.0.0-py2.py3-none-any.whl
pip install --editable .
```

##### 1.3.4. Set environment variables

```bash
export GOOGLE_APPLICATION_CREDENTIALS=datacatalog_credentials_file
```

> Replace above values according to your environment. The Data Catalog
> credentials file was saved in step 1.2.2.

### 1.4. Docker

See instructions below.

## 2. Sample application entry point

### 2.1. Run the looker2datacatalog script

- Virtualenv

```bash
looker2datacatalog \
  --datacatalog-project-id <YOUR-DATACATALOG-PROJECT-ID> \
  --looker-credentials-file looker_credentials_ini_file
```

- Or using Docker

```bash
docker build --rm --tag looker2datacatalog .
docker run --rm --tty -v <YOUR-CREDENTIALS_FILES_FOLDER>:/data \
  looker2datacatalog \ 
  --datacatalog-project-id <YOUR-DATACATALOG-PROJECT-ID> \
  --looker-credentials-file /data/looker2dc-looker-credentials.ini
```

## 3. Troubleshooting

In the case a connector execution hits Data Catalog quota limit, an error will
be raised and logged with the following detailment, depending on the performed
operation READ/WRITE/SEARCH: 

```
status = StatusCode.RESOURCE_EXHAUSTED
details = "Quota exceeded for quota metric 'Read requests' and limit 'Read requests per minute' of service 'datacatalog.googleapis.com' for consumer 'project_number:1111111111111'."
debug_error_string = 
"{"created":"@1587396969.506556000", "description":"Error received from peer ipv4:172.217.29.42:443","file":"src/core/lib/surface/call.cc","file_line":1056,"grpc_message":"Quota exceeded for quota metric 'Read requests' and limit 'Read requests per minute' of service 'datacatalog.googleapis.com' for consumer 'project_number:1111111111111'.","grpc_status":8}"
```

For more information on Data Catalog quota, please refer to: [Data Catalog quota docs][2].

[1]: https://github.com/looker-open-source/sdk-codegen/blob/master/looker-sample.ini
[2]: https://cloud.google.com/data-catalog/docs/resources/quotas
