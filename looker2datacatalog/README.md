# looker2datacatalog

Package for ingesting Looker metadata into Google Cloud Data Catalog.

**Disclaimer: This is not an officially supported Google product.**

## 1. Environment setup

### 1.1. Get the code

````bash
git clone https://.../looker2datacatalog.git
cd looker2datacatalog
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


[1]: https://github.com/looker-open-source/sdk-codegen/blob/master/looker-sample.ini

## 3. Troubleshooting

In the case a connector execution hits Data Catalog quota limit, an error will be raised and logged with the following detailement, depending on the performed operation READ/WRITE/SEARCH: 
```
status = StatusCode.RESOURCE_EXHAUSTED
details = "Quota exceeded for quota metric 'Read requests' and limit 'Read requests per minute' of service 'datacatalog.googleapis.com' for consumer 'project_number:1111111111111'."
debug_error_string = 
"{"created":"@1587396969.506556000", "description":"Error received from peer ipv4:172.217.29.42:443","file":"src/core/lib/surface/call.cc","file_line":1056,"grpc_message":"Quota exceeded for quota metric 'Read requests' and limit 'Read requests per minute' of service 'datacatalog.googleapis.com' for consumer 'project_number:1111111111111'.","grpc_status":8}"
```
For more info about Data Catalog quota, go to: [Data Catalog quota docs](https://cloud.google.com/data-catalog/docs/resources/quotas).