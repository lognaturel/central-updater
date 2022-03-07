# central-updater

Updates an entity list on one or more forms based on submissions that came in for one or more forms.

Verified with Python 3.8. To use:

* Copy `config.json.sample` to `config.json`
* Fill in your configuration
* Create a CSV with the initial state of the entities. **MUST** include all columns that are included in `config["updated_by"]`. MAY include additional columns
* Attach entity CSV to form with id listed in `config["entity"]["attached_to"][0]`
* Make sure forms with `form_id`s listed in `updated_by` all have a `select_one_from_file` over the entity list with field name `config["entity"]["key"]`
* Make sure forms with `form_id`s listed in the config all exist in `config["server"]["project"]`
* Run manually periodically and/or add to `crontab` (ideal is on the same network or machine as the Central server)

Creates a cache containing:
* An auth token. This token will be used for auth as long as the Central session is available
* A server last update date. Used to request submissions only since the last successful run

High-level flow:
* Verifies that the token is valid and auths if not
* Downloads submissions made to `updated_by` forms since the last cached date, exits if there are none
* Downloads the current entity list from `config["entity"]["attached_to"][0]`
* Uses `config["entity"]["key"]` to determine which entity was updated by each submission
* If there are multiple updates to a single entity, drops all except for the last one received (this is across all forms that update, even if there is no overlap in fields)
* Applies updates to the entity list
* Uploads updated list to all forms in `attached_to`
* Caches the datetime of the latest update

```
{
  "server": {
    "url": "",
    "username": "",
    "password": "",
    "project": 42
  },

  "entity": {
    "filename": "call_list.csv", <-- filename of the entity list
    "key": "interviewee", <-- name of field used to store the selected entity in form definitions

    "attached_to": [  <-- form ids of forms that the entity list is attached to
      "call_log",
      "survey"
    ],

    "updated_by": [ <-- form ids + fields of forms that update the entity list
      {
        "form_id": "call_log",
        "fields": [ <-- fields that get updated. Names must match between form def and entity list
          "status",
          "calls_made",
          "call_again",
          "finished_survey"
        ]
      }
    ]
  }
}
```