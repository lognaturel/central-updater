#!/usr/bin/env python3
import io
import sys
from logging.handlers import RotatingFileHandler
from urllib.parse import quote_plus
import logging

import requests
import json
import datetime as dt
from datetime import datetime
from dateutil.parser import isoparse
from typing import Optional
import pandas as pd
from pandas import DataFrame

CACHE = "cache.json"
CONFIG = "config.json"
LOG = "updater.log"

LOGGER = logging.getLogger("updater")


def configure_logger(filename):
    LOGGER.setLevel(logging.INFO)
    handler = RotatingFileHandler(filename, maxBytes=5 * 1024 * 1024, backupCount=2)
    formatter = logging.Formatter(fmt='%(asctime)s %(message)s', datefmt='%m/%d/%Y %I:%M:%S %p')
    handler.setFormatter(formatter)
    LOGGER.addHandler(handler)

    logging.basicConfig(level=logging.INFO, format='%(asctime)s %(message)s', datefmt='%m/%d/%Y %I:%M:%S %p')


def get_config(config_file: str):
    with open(config_file) as file:
        return json.load(file)


def get_token(server: dict, cache_file: Optional[str] = None) -> str:
    """Get a verified session token with the provided credential. First tries from cache if a cache file is provided,
    then falls back to requesting a new session. Updates cache if provided.

    Parameters:
        server: dict with the following keys:
            url: the base URL of the Central server to connect to
            username: the username of the Web User to auth with
            password: the Web User's password
        cache_file (optional): a file for caching the session token. This is recommended to minimize the login events logged
        on the server.

    Returns:
        Optional[str]: the session token or None if anything has gone wrong
    """
    token = get_verified_cached_token(server, cache_file) or get_new_token(server)

    if cache_file is not None:
        write_to_cache(cache_file, "token", token)

    if not token:
        raise SystemExit("Unable to get session token")

    return token


def get_verified_cached_token(server: dict, cache_file: Optional[str] = None) -> Optional[str]:
    if cache_file is None:
        return None

    try:
        with open(cache_file) as cache:
            cache = json.load(cache)
            token = cache["token"]
            # TODO: request updates with cached token to remove this request
            user_details_response = requests.get(
                f"{server['url']}/v1/users/current",
                headers={"Content-Type": "application/json", "Authorization": f"Bearer {token}"}
            )
            if user_details_response.ok:
                return token
    except (FileNotFoundError, KeyError):
        LOGGER.info("cache file missing or has no token key")


def get_new_token(server: dict) -> Optional[str]:
    email_token_response = requests.post(
        f"{server['url']}/v1/sessions",
        data=json.dumps({"email": server['username'], "password": server['password']}),
        headers={"Content-Type": "application/json"},
    )

    if email_token_response.status_code == 200:
        return email_token_response.json()["token"]


def write_to_cache(cache_file: str, key: str, value: str):
    """Add the given key/value pair to the provided cache file, preserving any other properties it may have"""
    try:
        with open(cache_file) as file:
            cache = json.load(file)
            cache[key] = value
    except FileNotFoundError:
        cache = {key: value}

    with open(cache_file, 'w') as outfile:
        json.dump(cache, outfile)


def get_last_update_timestamp(cache_file: str) -> str:
    try:
        with open(cache_file) as file:
            cache = json.load(file)
            return cache["last_open"]
    except FileNotFoundError:
        LOGGER.info("no cache file")
    except KeyError:
        LOGGER.info("no last_open cache key")
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
        LOGGER.warning(response.json())  # Something went wrong with the query
        return None

    return pd.json_normalize(response.json()['value'], sep='/')[updated_by['fields'] + [key, '__system/submissionDate']]


def get_entities(server: dict, token: str, form_id: str, filename: str) -> DataFrame:
    res = requests.get(f"{server['url']}/v1/projects/{server['project']}/forms/{form_id}/attachments/{filename}",
                       headers={"Content-Type": "application/json", "Authorization": f"Bearer {token}"}
                       )
    return pd.read_csv(io.BytesIO(res.content))


def upload(server: dict, token: str, attached_to: list, csv: str, filename: str):
    for dest in attached_to:
        draft = f"{server['url']}/v1/projects/{server['project']}/forms/{dest}/draft"
        requests.post(draft, headers={"Authorization": f"Bearer {token}"})
        requests.post(f"{draft}/attachments/{filename}", data=csv.encode('utf-8'), headers={"Content-Type": "text/csv", "Authorization": f"Bearer {token}"})
        requests.post(f"{draft}/publish?version={datetime.now().isoformat()}", headers={"Authorization": f"Bearer {token}"})


def main() -> int:
    configure_logger(LOG)
    LOGGER.info("##### Running #####")

    config = get_config(CONFIG)

    token = get_token(config['server'], cache_file=CACHE)

    last_update_timestamp = get_last_update_timestamp(CACHE)

    updates = get_updates(config['entity']['updated_by'], config['server'], token, last_update_timestamp, config['entity']['key'])

    if updates is None:
        LOGGER.info("No updates")
        return 0

    LOGGER.info(f"{updates.to_string(header=False)}")

    entities = get_entities(config['server'], token, config['entity']['attached_to'][0], config['entity']['filename']).set_index('name')
    entities.update(updates.set_index(config['entity']['key']))

    csv = entities.to_csv()
    upload(config['server'], token, config['entity']['attached_to'], csv, config['entity']['filename'])

    latest_update_timestamp = isoparse(updates['submissionDate'].max()) + dt.timedelta(milliseconds=1)
    LOGGER.debug(latest_update_timestamp)

    write_to_cache(CACHE, "last_open", latest_update_timestamp.isoformat())
    return 0


if __name__ == '__main__':
    sys.exit(main())
