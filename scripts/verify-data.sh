#!/bin/bash
echo "=== LOGS ==="
echo -n "VictoriaLogs:  "
curl -s 'http://victorialogs:9428/select/logsql/hits?query=*&start=2026-05-20T00:00:00Z&end=2026-05-23T00:00:00Z&step=72h' | grep -o '"total":[0-9]*'

echo -n "Lakehouse:     "
curl -s 'http://lakehouse-logs:9428/select/logsql/hits?query=*&start=2026-05-20T00:00:00Z&end=2026-05-23T00:00:00Z&step=72h' | grep -o '"total":[0-9]*'

echo -n "Loki:          "
LOKI_COUNT=$(curl -s 'http://loki:3100/loki/api/v1/index/stats' \
  --data-urlencode 'query={service_name=~".+"}' \
  --data-urlencode 'start=2026-05-20T00:00:00Z' \
  --data-urlencode 'end=2026-05-23T00:00:00Z' -G 2>/dev/null)
echo "$LOKI_COUNT"

echo ""
echo "=== TRACES ==="
echo -n "VictoriaTraces: "
VT_RESP=$(curl -s 'http://victoriatraces:10428/select/traces/spans?query=*&start=2026-05-20T00:00:00Z&end=2026-05-23T00:00:00Z&limit=1')
VT_COUNT=$(echo "$VT_RESP" | wc -l)
echo "response lines=$VT_COUNT (first: $(echo "$VT_RESP" | head -c 150))"

echo -n "Lakehouse:      "
LH_RESP=$(curl -s 'http://lakehouse-traces:10428/select/traces/spans?query=*&start=2026-05-20T00:00:00Z&end=2026-05-23T00:00:00Z&limit=1')
LH_COUNT=$(echo "$LH_RESP" | wc -l)
echo "response lines=$LH_COUNT (first: $(echo "$LH_RESP" | head -c 150))"

echo -n "Tempo:          "
TEMPO=$(curl -s 'http://tempo:3200/api/search?limit=1')
echo "$TEMPO" | grep -o '"inspectedTraces":[0-9]*'

echo ""
echo "=== TRACE SEARCH (Jaeger API) ==="
echo -n "VictoriaTraces: "
curl -s 'http://victoriatraces:10428/api/v3/traces/search?query=%7Bservice.name%3D~%22.%2B%22%7D&start=2026-05-20T00:00:00Z&end=2026-05-23T00:00:00Z&limit=1' | head -c 200
echo ""
echo -n "Lakehouse:      "
curl -s 'http://lakehouse-traces:10428/api/v3/traces/search?query=%7Bservice.name%3D~%22.%2B%22%7D&start=2026-05-20T00:00:00Z&end=2026-05-23T00:00:00Z&limit=1' | head -c 200
echo ""
