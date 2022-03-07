#!/usr/bin/env python3
import sys
from urllib.parse import quote_plus

import requests
import json
import datetime as dt
from datetime import datetime
from dateutil.parser import isoparse
from typing import Optional
import pandas as pd
from pandas import DataFrame


def get_config():
    with open('config.json') as config_file:
        return json.load(config_file)


def get_token(server: dict):
    token = get_verified_cached_token(server) or get_new_token(server)
    if not token:
        raise SystemExit("Unable to get session token")
    return token


def get_verified_cached_token(server: dict) -> Optional[str]:
    try:
        with open('cache.json') as cache_file:
            cache = json.load(cache_file)
            token = cache["token"]
            # TODO: request updates with cached token to remove this request
            user_details_response = requests.get(
                f"{server['url']}/v1/users/current",
                headers={"Content-Type": "application/json", "Authorization": f"Bearer {token}"}
            )
            if user_details_response.ok:
                return token
    except (FileNotFoundError, KeyError):
        print("cache file missing or has no token key")


def get_new_token(server: dict) -> Optional[str]:
    email_token_response = requests.post(
        f"{server['url']}/v1/sessions",
        data=json.dumps({"email": server['username'], "password": server['password']}),
        headers={"Content-Type": "application/json"},
    )

    if email_token_response.status_code == 200:
        return email_token_response.json()["token"]


def write_to_cache(key: str, value: str):
    try:
        with open('cache.json') as cache_file:
            cache = json.load(cache_file)
            cache[key] = value
    except FileNotFoundError:
        cache = {key: value}

    with open('cache.json', 'w') as outfile:
        json.dump(cache, outfile)


def get_last_update_timestamp() -> str:
    try:
        with open('cache.json') as cache_file:
            cache = json.load(cache_file)
            return cache["last_open"]
    except FileNotFoundError:
        print("no cache file")
    except KeyError:
        print("no last_open cache key")
    return "1970-01-01T00:00:00Z"


def get_updates(updated_by: list, server: dict, token: str, last_update_timestamp: str, key: str) -> Optional[DataFrame]:
    updates = None
    for update_form in updated_by:
        form_updates = get_form_updates(server, token, update_form, last_update_timestamp, key)
        if form_updates is not None:
            if updates is None:
                updates = form_updates
            else:
                updates = updates.merge(form_updates, how="outer")

    if updates is None:
        return None

    updates.rename(columns=lambda x: x.rsplit('/', 1)[-1], inplace=True)
    # TODO: this will mean the latest update across any form submission. Consider merging updates. groupby merge?
    return updates.sort_values('submissionDate', ascending=False).drop_duplicates(subset=[key], keep='first')


def get_form_updates(server: dict, token: str, updated_by: dict, last_update_timestamp: str, key: str) -> Optional[DataFrame]:
    response = requests.get(
        f"{server['url']}/v1/projects/{server['project']}/forms/{updated_by['form_id']}.svc/Submissions?$filter=__system/submissionDate gt {quote_plus(last_update_timestamp)} and __system/reviewState ne 'rejected'",
        headers={"Content-Type": "application/json", "Authorization": f"Bearer {token}"}
    )

    try:
        if len(response.json()['value']) == 0:
            return None
    except KeyError:
        print(response.json())  # Something went wrong with the query
        return None

    return pd.json_normalize(response.json()['value'], sep='/')[updated_by['fields'] + [key, '__system/submissionDate']]


def get_entities(server: dict, token: str, form_id: str, filename: str) -> DataFrame:
    return pd.read_csv(
        f"{server['url']}/v1/projects/{server['project']}/forms/{form_id}/attachments/{filename}",
        storage_options={"Authorization": f"Bearer {token}"}
    )


def upload(server: dict, token: str, attached_to: list, csv: str, filename: str):
    for dest in attached_to:
        draft = f"{server['url']}/v1/projects/{server['project']}/forms/{dest}/draft"
        requests.post(draft, headers={"Authorization": f"Bearer {token}"})
        requests.post(f"{draft}/attachments/{filename}", data=csv.encode('utf-8'), headers={"Content-Type": "text/csv", "Authorization": f"Bearer {token}"})
        requests.post(f"{draft}/publish?version={datetime.now().isoformat()}", headers={"Authorization": f"Bearer {token}"})


def main() -> int:
    config = get_config()

    token = get_token(config['server'])
    write_to_cache("token", token)

    last_update_timestamp = get_last_update_timestamp()

    updates = get_updates(config['entity']['updated_by'], config['server'], token, last_update_timestamp, config['entity']['key'])
    print(updates)

    if updates is None:
        print("No updates to process")
        return 0

    entities = get_entities(config['server'], token, config['entity']['attached_to'][0], config['entity']['filename']).set_index('name')
    entities.update(updates.set_index(config['entity']['key']))

    csv = entities.to_csv()
    upload(config['server'], token, config['entity']['attached_to'], csv, config['entity']['filename'])

    latest_update_timestamp = isoparse(updates['submissionDate'].max()) + dt.timedelta(milliseconds=1)
    print(latest_update_timestamp)

    write_to_cache("last_open", latest_update_timestamp.isoformat())
    return 0


if __name__ == '__main__':
    sys.exit(main())
