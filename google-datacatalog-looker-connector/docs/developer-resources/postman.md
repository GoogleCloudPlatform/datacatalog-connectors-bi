# Postman

Developers may leverage [Postman](https://www.postman.com/) to validate API
requests. The available collection covers the Looker REST API.  

## Collection for the Looker REST API

1. File: [tools/postman/Looker REST API.postman_collection.json](../../tools/postman/Looker%20REST%20API.postman_collection.json)

1. Postman Environment Variables:

   | NAME                | DESCRIPTION                                                                |
   | ------------------- | -------------------------------------------------------------------------- |
   | `PROTOCOL`          | `http` or `https`                                                          |
   | `SERVER`            | URL of the Looker server                                                   |
   | `API_VERSION`       | Version of the Looker REST API, e.g. `3.1`                                 |
   | `CLIENT_ID`         | Client ID to authenticate the API calls                                    |
   | `CLIENT_SECRET`     | Client Secret to authenticate the API calls                                |
   | `QUERY_ID`          | ID of a Query to fetch its metadata, including the generated SQL statement |
   | `LOOKML_MODEL_NAME` | Name of a LookML Model to fetch metadata from                              |
   | `CONNECTION_NAME`   | Name of a Connection to fetch metadata from                                |
