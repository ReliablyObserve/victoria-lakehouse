"""Runs inside the Spark container: executes the materialised doc snippets of the job file in one
JVM and prints the answers as one JSON document after the @@RESULT@@ marker."""
import json
import sys

import engines


def main():
    job = json.load(open(sys.argv[1]))
    out = {}
    for item in job["cells"]:
        try:
            got = engines.exec_body(item["body"], "spark")
            out[item["cell"]] = {k: ({"error": str(v)} if isinstance(v, engines.QueryError) else {"value": v}) for k, v in got.items()}
        except Exception as e:  # setup failure
            out[item["cell"]] = {"__error__": "%s: %s" % (type(e).__name__, str(e)[:500])}
    print("@@RESULT@@" + json.dumps(out))


if __name__ == "__main__":
    main()
