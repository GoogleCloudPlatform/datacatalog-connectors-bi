# Design decisions

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
approach: the endpoint needs admin license rights in the API version we are
using as reference, `Windows 8.2.5.11026 v1`, and we get a `403 - Access denied`
when the authenticated user does not have such permission. In this case, we
are going to skip fulfilling the assets' ownership metadata.

---

Back to the [Developer Resources index](..)
