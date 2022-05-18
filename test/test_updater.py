import pytest
import mock

from updater import *


@mock.patch("builtins.open", mock.mock_open(read_data='{"foo": "bar"}'))
def test_get_config__returns_dict_from_json():
    assert get_config("config.json") == {'foo': 'bar'}


@mock.patch("builtins.open", side_effect=FileNotFoundError)
def test_get_config__raises_error__when_file_not_found(open_mock):
    with pytest.raises(FileNotFoundError):
        get_config("config.json")


@mock.patch("builtins.open", mock.mock_open(read_data='{"token": "foo"}'))
@mock.patch("requests.get")
def test_get_verified_cached_token__returns_cached_token__when_token_valid(request_mock):
    request_mock.return_value.ok = True
    assert get_verified_cached_token({"url": "bar"}, "cache") == "foo"


@mock.patch("builtins.open", mock.mock_open(read_data='{"token": "foo"}'))
@mock.patch("requests.get")
def test_get_verified_cached_token__returns_none__when_token_not_valid(request_mock):
    request_mock.return_value.ok = False
    assert get_verified_cached_token({"url": "bar"}, "cache") is None


@mock.patch("builtins.open", side_effect=FileNotFoundError)
def test_get_verified_cached_token__returns_none__when_no_cache_file(open_mock):
    assert get_verified_cached_token({"url": "bar"}, "cache") is None


@mock.patch("builtins.open", mock.mock_open(read_data='{"foo": "bar"}'))
@mock.patch("requests.get")
def test_get_verified_cached_token__returns_none__when_cache_file_corrupt(request_mock):
    request_mock.return_value.ok = True
    assert get_verified_cached_token({"url": "bar"}, "cache") is None


@mock.patch("updater.get_verified_cached_token")
def test_get_token__returns_cached_token__when_cached_token_verified(verify_fn):
    verify_fn.return_value = "foo"
    assert get_token({"url": "bar"}, "cache") == "foo"


@mock.patch("updater.get_new_token")
@mock.patch("updater.get_verified_cached_token")
def test_get_token__does_not_request_new_token__when_cached_token_verified(verify_fn, get_new_fn):
    verify_fn.return_value = "foo"
    assert not get_new_fn.called


@mock.patch("updater.get_new_token")
@mock.patch("updater.get_verified_cached_token")
def test_get_token__requests_new_token__when_no_cached_verified_token(verify_fn, get_new_fn):
    verify_fn.return_value = None
    get_new_fn.return_value = "foo"
    assert get_token({"url": "bar"}) == "foo"


@mock.patch("updater.get_new_token")
@mock.patch("updater.get_verified_cached_token")
def test_get_token__raises_error__when_no_token(verify_fn, get_new_fn):
    verify_fn.return_value = None
    get_new_fn.return_value = None
    with pytest.raises(SystemExit):
        get_token({"url": "bar"})


def mocked_open(*args):
    if len(args) == 1:
        raise FileNotFoundError
    else:
        return mock.mock_open()


@mock.patch("json.dump")
@mock.patch("builtins.open")
def test_write_to_cache__creates_file__when_no_file_exists(open_mock, json_mock):
    open_mock.side_effect = mocked_open
    open_mock.__enter__ = mock.Mock(return_value=(mock.Mock(), None))
    open_mock.__exit__ = mock.Mock(return_value=None)
    write_to_cache("cache", "foo", "bar")
    json_mock.assert_called_with({'foo': 'bar'}, mock.ANY)


@mock.patch("json.dump")
@mock.patch("builtins.open", mock.mock_open(read_data='{"foo": "foo"}'))
def test_write_to_cache__replaces_token__when_file_with_token_exists(json_mock):
    write_to_cache("cache", "foo", "bar")
    json_mock.assert_called_with({'foo': 'bar'}, mock.ANY)


@mock.patch("json.dump")
@mock.patch("builtins.open", mock.mock_open(read_data='{"foo": "bar"}'))
def test_write_to_cache__preserves_other_keys__when_file_with_other_keys_exists(json_mock):
    write_to_cache("cache", "bar", "baz")
    json_mock.assert_called_with({'bar': 'baz', 'foo': 'bar'}, mock.ANY)

