#!/usr/bin/env python3
"""Seed one visual-proof stack: cold rows, graceful Lakehouse restart, same-hour rows.
usage: seed.py <project> <port_base> <logs_image> <traces_image> [--down]"""
import json, subprocess, sys, time, urllib.request, os, datetime
project, base, logs_img, traces_img = sys.argv[1], int(sys.argv[2]), sys.argv[3], sys.argv[4]
env = dict(os.environ, LOGS_IMAGE=logs_img, TRACES_IMAGE=traces_img, PORT_BASE=str(base))
U = {"lh_logs": f"http://127.0.0.1:{base}1", "lh_traces": f"http://127.0.0.1:{base}2", "grafana": f"http://127.0.0.1:{base}3",
     "hot_logs": f"http://127.0.0.1:{base}4", "hot_traces": f"http://127.0.0.1:{base}5"}
HDR = {"AccountID": "0", "ProjectID": "0"}
N = 20       # log rows per phase
T = 5        # traces per phase (3 spans each)

def log(*a): print(datetime.datetime.now(datetime.timezone.utc).strftime("%H:%M:%S"), *a, flush=True)
def compose(*args):
    r = subprocess.run(["docker", "compose", "-p", project, "-f", "/tmp/fix272-vis/compose.yml", *args], env=env, capture_output=True, text=True)
    if r.returncode != 0: raise SystemExit(f"compose {args}: {r.stderr[-600:]}")
    return r.stdout
def http(method, url, body=None, ctype=None):
    req = urllib.request.Request(url, data=body, method=method)
    for k, v in HDR.items(): req.add_header(k, v)
    if ctype: req.add_header("Content-Type", ctype)
    try:
        with urllib.request.urlopen(req, timeout=30) as r: return r.status, r.read().decode()
    except urllib.error.HTTPError as e: return e.code, e.read().decode()
    except Exception as e: return 0, str(e)
def wait(url, path="/ready"):
    for _ in range(120):
        if http("GET", url + path)[0] == 200: return
        time.sleep(1)
    raise SystemExit(url + " not ready")
def iso(ns): return datetime.datetime.fromtimestamp(ns / 1e9, datetime.timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.%f") + "Z"
def tid(layer, i): return (layer[0] * 2 + f"{i:02d}").ljust(32, "a")
def sid(layer, i, j): return (layer[0] + f"{i}{j}").ljust(16, "b")

def write(layer, ts_ns, targets):
    lines = "".join(json.dumps({"_time": iso(ts_ns + i * 1_000_000), "_msg": f"repro {layer} line {i} trace_id={tid(layer, i % T)}", "repro_layer": layer,
                                "service.name": "repro", "trace_id": tid(layer, i % T), "level": "info"}) + "\n" for i in range(N))
    spans = []
    for i in range(T):
        for j in range(3):
            s = ts_ns + (i * 3 + j) * 1_000_000
            spans.append({"resource": {"attributes": [{"key": "service.name", "value": {"stringValue": "repro"}}]},
                          "scopeSpans": [{"scope": {"name": "repro"}, "spans": [{
                              "traceId": tid(layer, i), "spanId": sid(layer, i, j), "parentSpanId": "" if j == 0 else sid(layer, i, 0),
                              "name": f"repro-{layer}-{j}", "kind": 2, "startTimeUnixNano": str(s), "endTimeUnixNano": str(s + 900_000),
                              "attributes": [{"key": "repro_layer", "value": {"stringValue": layer}}]}]}]})
    for name in targets:
        lurl, turl = (U["lh_logs"], U["lh_traces"]) if name == "lh" else (U["hot_logs"], U["hot_traces"])
        for _ in range(15):
            st, b = http("POST", lurl + "/insert/jsonline?_stream_fields=repro_layer,service.name", lines.encode(), "application/stream+json")
            if st == 200: break
            time.sleep(3)
        for _ in range(15):
            st2, b2 = http("POST", turl + "/insert/opentelemetry/v1/traces", json.dumps({"resourceSpans": spans}).encode(), "application/json")
            if st2 == 200: break
            time.sleep(3)
        log(f"{name} write {layer} at {iso(ts_ns)}: logs={st} traces={st2}")

if "--down" in sys.argv:
    compose("down", "-v", "--remove-orphans", "-t", "20"); sys.exit(0)
now = datetime.datetime.now(datetime.timezone.utc)
if now.minute >= 52: raise SystemExit("too close to the hour boundary; rerun after :00")
compose("up", "-d", "--no-build")
for k in ("lh_logs", "lh_traces"): wait(U[k])
for k in ("hot_logs",): wait(U[k], "/health")
time.sleep(3)
t0 = time.time_ns()
write("cold", t0 - 20_000_000_000, ["lh", "hot"])
time.sleep(2)
log("graceful restart of lakehouse-logs and lakehouse-traces")
compose("restart", "-t", "90", "lakehouse-logs", "lakehouse-traces")
for k in ("lh_logs", "lh_traces"): wait(U[k])
time.sleep(3)
t1 = time.time_ns()
write("buffer", t1 - 5_000_000_000, ["lh", "hot"])
log("waiting 40s for the latency offset and Grafana")
time.sleep(40)
wait(U["grafana"], "/api/health")
json.dump({"urls": U, "cold_ns": t0 - 20_000_000_000, "buffer_ns": t1 - 5_000_000_000, "N": N, "T": T}, open(f"/tmp/fix272-vis/{project}.json", "w"))
log("ready", U)
