import urllib.request, urllib.parse, json, sys, pathlib
variant=sys.argv[1]
result={}
wanted={"lakehouse_startup_phase","lakehouse_startup_total_seconds","lakehouse_ready","lakehouse_serving_ready","lakehouse_warmup_complete"}
for signal, port in [("logs",39901),("traces",39902)]:
 base=f"http://127.0.0.1:{port}"
 with urllib.request.urlopen(base+"/lakehouse/info") as response:
  info=json.load(response); status=response.status
 with urllib.request.urlopen(base+"/ready") as response:
  ready={"status":response.status,"body":response.read().decode().strip()}
 with urllib.request.urlopen(base+"/metrics") as response:
  lines=response.read().decode().splitlines()
 metrics={line.split()[0]:float(line.split()[1]) for line in lines if len(line.split())==2 and line.split()[0] in wanted}
 query='{run="startup-proof"} | stats count() c' if signal=="logs" else '"span_attr:run":"startup-proof" | stats count() c'
 params=urllib.parse.urlencode({"query":query,"start":"0","end":"2100000000","disable_latency_offset":"true"})
 with urllib.request.urlopen(base+"/select/logsql/query?"+params) as response:
  control={"status":response.status,"body":response.read().decode()}
 result[signal]={"info_status":status,"info":{k:info[k] for k in ["mode","phase","ready"]},"ready":ready,"metrics":metrics,"query_control":control}
pathlib.Path(__file__).with_name(variant+".json").write_text(json.dumps(result,indent=2))
print(json.dumps(result,indent=2))
