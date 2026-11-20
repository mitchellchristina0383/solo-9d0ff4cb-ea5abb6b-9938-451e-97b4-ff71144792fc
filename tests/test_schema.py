import pytest
from fastapi.testclient import TestClient

from app.db import Database
from app.main import create_app


def test_newer_schema_is_refused(tmp_path):
    db_path = tmp_path / "newer.db"
    db = Database(str(db_path))
    db.initialise()
    with db.write() as conn:
        conn.execute("UPDATE meta SET value = '99' WHERE key = 'schema_version'")
    db.close_thread_connection()

    with pytest.raises(RuntimeError, match="newer than this service supports"):
        with TestClient(create_app(str(db_path))):
            pass


def test_default_db_path_env(monkeypatch):
    from app import db as db_module

    monkeypatch.setenv("BLOCKSTORE_DB", "/tmp/custom-path.db")
    assert db_module.database_path_from_env() == "/tmp/custom-path.db"
    monkeypatch.setenv("BLOCKSTORE_BUSY_TIMEOUT_SECONDS", "7.5")
    assert db_module.busy_timeout_from_env() == 7.5
    monkeypatch.setenv("BLOCKSTORE_BUSY_TIMEOUT_SECONDS", "not-a-number")
    assert db_module.busy_timeout_from_env() == 30.0
