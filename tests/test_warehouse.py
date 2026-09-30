import common.warehouse as wh


def test_configured_key_wins_when_it_exists(tmp_path, monkeypatch):
    key = tmp_path / "key.json"
    key.write_text("{}")
    monkeypatch.setenv("GOOGLE_APPLICATION_CREDENTIALS", str(key))
    assert wh.credentials_path() == str(key)


def test_placeholder_path_falls_back_to_include_key(tmp_path, monkeypatch):
    fallback = tmp_path / "gcp-key.json"
    fallback.write_text("{}")
    monkeypatch.setattr(wh, "DEFAULT_KEY_FILE", fallback)
    monkeypatch.setenv("GOOGLE_APPLICATION_CREDENTIALS", "/path/to/service-account.json")
    assert wh.credentials_path() == str(fallback)


def test_no_key_anywhere(tmp_path, monkeypatch):
    monkeypatch.setattr(wh, "DEFAULT_KEY_FILE", tmp_path / "missing.json")
    monkeypatch.delenv("GOOGLE_APPLICATION_CREDENTIALS", raising=False)
    assert wh.credentials_path() is None
