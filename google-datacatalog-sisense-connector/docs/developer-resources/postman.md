# Postman

Developers may leverage [Postman](https://www.postman.com/) to validate API
requests. The available collection covers the [Sisense REST API
v1](https://sisense.dev/reference/rest/v1.html).

## Collection for the Sisense REST API v1

1. File: [tools/postman/Sisense REST API v1.postman_collection.json](../../tools/postman/Sisense%20REST%20%20API%20v1.postman_collection.json)

1. Postman Environment Variables:

   | NAME          | DESCRIPTION                                |
   | ------------- | ------------------------------------------ |
   | `SCHEME`      | The URI scheme: `http` or `https`          |
   | `SERVER`      | URL of the Sisense server                  |
   | `API_VERSION` | Version of the Sisense REST API, e.g. `v1` |
   | `USERNAME`    | Username to authenticate the API calls     |
   | `PASSWORD`    | Password to authenticate the API calls     |

1. Sample environment file: [tools/postman/sisense-sample-environment.postman_environment.json](../../tools/postman/sisense-sample-environment.postman_environment.json)

---

Back to the [Developer Resources index](..)
