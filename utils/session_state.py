"""Atomic auto-session checkpoints, isolated by account and run settings."""
import fcntl
import hashlib
import json
import os
import tempfile
from pathlib import Path


def atomic_json(path, payload):
    path = Path(path)
    fd, temporary = tempfile.mkstemp(dir=path.parent, prefix=".checkpoint-")
    try:
        with os.fdopen(fd, "w", encoding="utf-8") as stream:
            json.dump(payload, stream, sort_keys=True, allow_nan=False)
            stream.write("\n")
            stream.flush()
            os.fsync(stream.fileno())
        os.replace(temporary, path)
    finally:
        if os.path.exists(temporary):
            os.unlink(temporary)


class SessionState:
    def __init__(self, root, settings, account, *, new_session=False):
        root = Path(root).expanduser().resolve()
        root.mkdir(parents=True, exist_ok=True)
        ignored = {"cli_settings", "new_session", "session_log_dir", "trade_charts", "risk_session_log"}
        identity = {key: value for key, value in settings.items() if key not in ignored}
        identity["account"] = str(account).lower()
        key = hashlib.sha256(json.dumps(identity, sort_keys=True).encode()).hexdigest()
        self.path = root / (".active-" + key + ".json")
        self.lock = (root / (".active-" + key + ".lock")).open("a")
        try:
            fcntl.flock(self.lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
            self.data = json.loads(self.path.read_text()) if self.path.exists() and not new_session else {}
            if self.data and self.data.get("schema_version") != 1:
                raise RuntimeError(f"Unsupported session checkpoint: {self.path}")
            if self.data.get("complete"):
                self.data = {}
        except BaseException:
            self.close()
            raise

    def save(self):
        self.data["schema_version"] = 1
        atomic_json(self.path, self.data)

    def close(self):
        self.lock.close()
