# Design decisions

## Assets identification: `_id` vs `oid`

Many Sisense assets have two unique identifiers: `_id` and `oid`. We asked
Sisense folks about the purpose of these fields concerning folders and
dashboards, and got the below answer:

> The `oid` is the ID used for tied dashboard entity and folder entity while
`_id` is just an ID used within our application database as it is required to
have a unique ID for each record (collection) basically. This said, if you plan
to use the API you can ignore the `_id` as it is not used for the application
logic. The dashboards have a parameter called `parentFolder` which gets the
folder `oid` in it whenever you create a dashboard inside folder.

That being said, we decided to use `oid` and ignore `_id` when both fields are
available for the same entity (no matter its type). The chosen ID field is used
to build linked resources (URLs that connect Data Catalog entries to Sisense UI
elements) and parent-child relationships through Data Catalog tags.

## Assets' ownership metadata handling

Folders, dashboards, and widgets have an `owner` field, returned in the format
of an ID by the API. There is more than one way to use such ID to retrieve the
owner's username, first name, and last name, which are human-friendly metadata,
but the API behaves differently depending on the asset type:

1. **Folder**: we can append an `expand=owner` parameter to the `GET /folders`
   query string. The API then replaces the owner ID with a `user` object with
   plenty of metadata (much more than we need). There is a side-effect, though:
   each folder object turns ~200KB length against ~1KB when the expand option
   is not provided. We are concerned about the impact when synchronizing big
   Sisense sites with Google Data Catalog, especially because we don't need any
   other user's fields but the ones mentioned before.
1. **Dashboard**: we can append the `expand=owner` parameter to the
   `GET /dashboards` query string. The API then replaces the ID with a `user`
   object, but in this case, the object contains only a few fields, including
   the ones we are looking for. This is exactly what we need!
1. **Widget**: there's no way to expand the `owner` field, as far as we know.


That being said, we decided to avoid expanding the objects and use the 
`GET /users/{id}` endpoint to retrieve information on each asset owner. A cache
can be used to decrease the number of API calls. There is a tradeoff for this
approach: the endpoint needs admin license rights in the REST API version we
are taking for reference, `Windows 8.2.5 v1`, and we get a
`403 - Access denied` when the authenticated user does not have such
permission. We skip fulfilling the assets' ownership metadata when it happens.

---

Back to the [Documentation index](README.md)
