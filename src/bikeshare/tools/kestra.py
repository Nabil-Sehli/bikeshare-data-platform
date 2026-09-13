"""Deploy Kestra flows from kestra/flows/ and optionally trigger one.

Usage:
    python -m bikeshare.tools.kestra deploy
    python -m bikeshare.tools.kestra run platform_backfill --input city=JC --input start_month=2025-01 [--wait]
"""

from __future__ import annotations

import argparse
import sys
import time
from pathlib import Path

import requests
import yaml

from bikeshare.config import env

TENANT = "main"
FLOWS_DIR = Path(__file__).resolve().parents[3] / "kestra" / "flows"
TERMINAL_STATES = {"SUCCESS", "WARNING", "FAILED", "KILLED", "CANCELLED"}


def session() -> tuple[requests.Session, str]:
    s = requests.Session()
    s.auth = (env("KESTRA_USER"), env("KESTRA_PASSWORD"))
    base = env("KESTRA_URL", "http://localhost:8080").rstrip("/") + f"/api/v1/{TENANT}"
    for attempt in range(60):
        try:
            if s.get(f"{base}/flows/search", params={"size": 1}, timeout=10).status_code == 200:
                return s, base
        except requests.RequestException:
            pass
        print(f"waiting for Kestra API ({attempt + 1}/60)...")
        time.sleep(5)
    sys.exit("Kestra API not reachable")


def deploy() -> None:
    s, base = session()
    headers = {"Content-Type": "application/x-yaml"}
    files = sorted(FLOWS_DIR.glob("*.yml"))
    for path in files:
        source = path.read_text(encoding="utf-8")
        meta = yaml.safe_load(source)
        ns, flow_id = meta["namespace"], meta["id"]
        resp = s.put(f"{base}/flows/{ns}/{flow_id}", data=source.encode(), headers=headers, timeout=30)
        if resp.status_code == 404:
            resp = s.post(f"{base}/flows", data=source.encode(), headers=headers, timeout=30)
        if resp.status_code >= 400:
            sys.exit(f"failed to deploy {path.name}: {resp.status_code} {resp.text[:800]}")
        print(f"deployed {ns}.{flow_id} (revision {resp.json().get('revision')})")
    print(f"{len(files)} flows deployed -> http://localhost:8080/ui/main/flows?namespace=bikeshare")


def run(flow_id: str, inputs: list[str], namespace: str, wait: bool) -> None:
    s, base = session()
    form = dict(i.split("=", 1) for i in inputs)
    resp = s.post(f"{base}/executions/{namespace}/{flow_id}", files={k: (None, v) for k, v in form.items()} or None,
                  timeout=30)
    if resp.status_code >= 400:
        sys.exit(f"failed to start {flow_id}: {resp.status_code} {resp.text[:800]}")
    execution_id = resp.json()["id"]
    print(f"started {namespace}.{flow_id}: http://localhost:8080/ui/main/executions/{namespace}/{flow_id}/{execution_id}")
    if not wait:
        return
    state, last = None, None
    while state not in TERMINAL_STATES:
        time.sleep(10)
        execution = s.get(f"{base}/executions/{execution_id}", timeout=30).json()
        state = execution["state"]["current"]
        running = [t["taskId"] for t in execution.get("taskRunList") or [] if t["state"]["current"] == "RUNNING"]
        if (state, running) != last:
            print(f"  {time.strftime('%H:%M:%S')} {state} {', '.join(running)}")
            last = (state, running)
    if state not in {"SUCCESS", "WARNING"}:
        sys.exit(f"execution finished with state {state}")


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    sub = parser.add_subparsers(dest="command", required=True)
    sub.add_parser("deploy")
    run_p = sub.add_parser("run")
    run_p.add_argument("flow_id")
    run_p.add_argument("--namespace", default="bikeshare")
    run_p.add_argument("--input", action="append", default=[], help="key=value")
    run_p.add_argument("--wait", action="store_true")
    args = parser.parse_args()

    if args.command == "deploy":
        deploy()
    else:
        run(args.flow_id, args.input, args.namespace, args.wait)


if __name__ == "__main__":
    main()
