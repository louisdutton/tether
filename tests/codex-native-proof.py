import argparse
import json
import os
import pathlib
import selectors
import sqlite3
import subprocess
import tempfile
import time

parser = argparse.ArgumentParser(description="Disposable Codex catalog and permanent-deletion integration proof")
parser.add_argument("--codex", required=True, help="Absolute native Codex executable")
parser.add_argument("--adapter", required=True, help="Absolute patched Codex ACP executable")
parser.add_argument("--tether", required=True, help="Absolute Tether executable")
args = parser.parse_args()
CODEX, ADAPTER, TETHER = (str(pathlib.Path(p).resolve(strict=True)) for p in (args.codex, args.adapter, args.tether))
root = pathlib.Path(tempfile.mkdtemp(prefix="tether-catalog-native-proof-"))
home = root / "codex-home"
home.mkdir()
workspace = root / "workspace"
workspace.mkdir()
(home / "config.toml").write_text(
    'model="fixture"\nmodel_provider="proof"\n'
    '[model_providers.proof]\nname="Proof"\n'
    'base_url="http://127.0.0.1:1"\nenv_key="TETHER_DELETE_PROOF_KEY"\n'
    'wire_api="responses"\nrequest_max_retries=0\nstream_max_retries=0\n'
)
env = {
    **os.environ,
    "CODEX_HOME": str(home),
    "CODEX_PATH": CODEX,
    "TETHER_DELETE_PROOF_KEY": "disposable-local-test",
    "OPENAI_API_KEY": "disposable-local-test",
}


class Peer:
    def __init__(self, command, environment):
        self.log = tempfile.TemporaryFile()
        self.p = subprocess.Popen(
            command, stdin=subprocess.PIPE, stdout=subprocess.PIPE,
            stderr=self.log, env=environment,
        )
        self.buffer = b""
        self.next_id = 0

    def send(self, obj):
        self.p.stdin.write(json.dumps(obj).encode() + b"\n")
        self.p.stdin.flush()

    def line(self, deadline):
        while b"\n" not in self.buffer:
            with selectors.DefaultSelector() as sel:
                sel.register(self.p.stdout, selectors.EVENT_READ)
                if not sel.select(max(0, deadline - time.monotonic())):
                    self.log.seek(0)
                    raise TimeoutError(self.log.read().decode()[-2000:])
            chunk = os.read(self.p.stdout.fileno(), 65536)
            if not chunk:
                self.log.seek(0)
                raise EOFError(self.log.read().decode()[-2000:])
            self.buffer += chunk
        line, self.buffer = self.buffer.split(b"\n", 1)
        return json.loads(line)

    def call(self, method, params, timeout=45):
        self.next_id += 1
        request_id = self.next_id
        self.send({"jsonrpc": "2.0", "id": request_id, "method": method, "params": params})
        deadline = time.monotonic() + timeout
        while True:
            message = self.line(deadline)
            if message.get("id") == request_id:
                assert "error" not in message, (method, message)
                return message["result"]

    def close(self):
        self.p.stdin.close()
        try:
            self.p.wait(timeout=10)
        except subprocess.TimeoutExpired:
            self.p.terminate()
            self.p.wait(timeout=10)


native = Peer([CODEX, "app-server"], env)
native.call("initialize", {"clientInfo": {"name": "tether-catalog-proof", "version": "1.0"}})
native.send({"method": "initialized"})
original_ids = []
try:
    for index in range(3):
        thread = native.call("thread/start", {"cwd": str(workspace), "ephemeral": False})["thread"]
        original_ids.append(thread["id"])
        native.call("turn/start", {
            "threadId": thread["id"],
            "input": [{"type": "text", "text": "Disposable native history proof", "text_elements": []}],
        })
        deadline = time.monotonic() + 15
        while not pathlib.Path(thread["path"]).exists():
            assert time.monotonic() < deadline
            time.sleep(0.05)
        time.sleep(0.5)
        if index:
            native.call("thread/archive", {"threadId": thread["id"]})
finally:
    native.close()

assert len(list(home.glob("**/rollout-*.jsonl"))) == 3
agents = [{
    "id": "codex", "command": ADAPTER, "args": [],
    "permanentDelete": {"method": "_tether/session/delete_permanently"},
}]
tether_env = {
    **env, "TETHER_WORKSPACE_ROOT": str(workspace), "TETHER_DATA_DIR": str(root / "tether"),
    "TETHER_AGENTS": json.dumps(agents),
}


def connect():
    peer = Peer([TETHER, "--stdio"], tether_env)
    peer.call("initialize", {"protocolVersion": 1, "clientCapabilities": {}})
    return peer


peer = connect()
try:
    catalog = peer.call("session/list", {})
    assert catalog["_meta"]["tether"]["catalogComplete"], catalog
    sessions = catalog["sessions"]
    assert len(sessions) == 3, catalog
    mapping = {s["_meta"]["tether"]["bindings"][0]["session_id"]: s["sessionId"] for s in sessions}
    assert set(mapping) == set(original_ids)
    print("Native active + archived catalog: 3 conversations, one public ID each", flush=True)
finally:
    peer.close()

peer = connect()
try:
    catalog = peer.call("session/list", {})
    restored = {s["_meta"]["tether"]["bindings"][0]["session_id"]: s["sessionId"] for s in catalog["sessions"]}
    assert mapping == restored
    print("Catalog IDs stable across Tether restart", flush=True)
    archived_id = mapping[original_ids[-1]]
    params = {"sessionId": archived_id, "cwd": str(workspace), "mcpServers": []}
    peer.call("session/load", params)
    peer.call("session/close", {"sessionId": archived_id})
    peer.call("session/resume", params)
    peer.call("session/close", {"sessionId": archived_id})
    print("Archived conversation load/close/resume: passed", flush=True)
    for native_id in original_ids:
        peer.call("session/delete", {"sessionId": mapping[native_id]})
    assert peer.call("session/list", {})["sessions"] == []
    assert not list(home.glob("**/rollout-*.jsonl"))
    # The provider response and original files must agree with Tether's catalog.
    for native_id in original_ids:
        assert not list(home.glob(f"**/*{native_id}*.jsonl"))
    print("Permanent deletion: active/archived native histories and Tether catalog removed", flush=True)
finally:
    peer.close()

with sqlite3.connect(root / "tether" / "sessions.sqlite3") as db:
    for table in ("sessions", "turns", "inputs"):
        assert db.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0] == 0

(root / "result.json").write_text(json.dumps({"nativeIds": original_ids, "catalogIds": mapping, "success": True}))
print("Proof artifacts:", root, flush=True)
