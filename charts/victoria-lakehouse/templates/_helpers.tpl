{{/*
Chart name
*/}}
{{- define "victoria-lakehouse.name" -}}
{{- default .Chart.Name .Values.nameOverride | trunc 63 | trimSuffix "-" }}
{{- end }}

{{/*
Full resource name
*/}}
{{- define "victoria-lakehouse.fullname" -}}
{{- if .Values.fullnameOverride }}
{{- .Values.fullnameOverride | trunc 63 | trimSuffix "-" }}
{{- else }}
{{- $name := default .Chart.Name .Values.nameOverride }}
{{- if contains $name .Release.Name }}
{{- .Release.Name | trunc 63 | trimSuffix "-" }}
{{- else }}
{{- printf "%s-%s" .Release.Name $name | trunc 63 | trimSuffix "-" }}
{{- end }}
{{- end }}
{{- end }}

{{/*
Chart label value
*/}}
{{- define "victoria-lakehouse.chart" -}}
{{- printf "%s-%s" .Chart.Name .Chart.Version | replace "+" "_" | trunc 63 | trimSuffix "-" }}
{{- end }}

{{/*
Common labels
*/}}
{{- define "victoria-lakehouse.labels" -}}
helm.sh/chart: {{ include "victoria-lakehouse.chart" . }}
{{ include "victoria-lakehouse.selectorLabels" . }}
{{- if .Chart.AppVersion }}
app.kubernetes.io/version: {{ .Chart.AppVersion | quote }}
{{- end }}
app.kubernetes.io/managed-by: {{ .Release.Service }}
{{- with .Values.global.commonLabels }}
{{ toYaml . }}
{{- end }}
{{- end }}

{{/*
Selector labels
*/}}
{{- define "victoria-lakehouse.selectorLabels" -}}
app.kubernetes.io/name: {{ include "victoria-lakehouse.name" . }}
app.kubernetes.io/instance: {{ .Release.Name }}
{{- end }}

{{/*
Component labels — signal-aware.
Usage: {{ include "victoria-lakehouse.componentLabels" (dict "root" . "signal" "logs" "role" "select") }}
*/}}
{{- define "victoria-lakehouse.componentLabels" -}}
{{ include "victoria-lakehouse.labels" .root }}
app.kubernetes.io/component: {{ .signal }}-{{ .role }}
app.kubernetes.io/signal: {{ .signal }}
{{- end }}

{{/*
Component selector labels — signal-aware.
Usage: {{ include "victoria-lakehouse.componentSelectorLabels" (dict "root" . "signal" "logs" "role" "select") }}
*/}}
{{- define "victoria-lakehouse.componentSelectorLabels" -}}
{{ include "victoria-lakehouse.selectorLabels" .root }}
app.kubernetes.io/component: {{ .signal }}-{{ .role }}
app.kubernetes.io/signal: {{ .signal }}
{{- end }}

{{/*
Container image for a signal.
Usage: {{ include "victoria-lakehouse.signalImage" (dict "root" . "signal" "logs") }}
*/}}
{{- define "victoria-lakehouse.signalImage" -}}
{{- $repository := .root.Values.image.logs.repository -}}
{{- if eq .signal "traces" -}}
{{- $repository = .root.Values.image.traces.repository -}}
{{- end -}}
{{- $defaultTag := .root.Chart.AppVersion -}}
{{- $publishedRepository := printf "ghcr.io/reliablyobserve/victoria-lakehouse/lakehouse-%s" .signal -}}
{{- if eq $repository $publishedRepository -}}
{{- $defaultTag = printf "v%s" .root.Chart.AppVersion -}}
{{- end -}}
{{- printf "%s:%s" $repository (default $defaultTag .root.Values.image.tag) -}}
{{- end }}

{{/*
Service port for a signal: 9428 for logs, 10428 for traces.
Usage: {{ include "victoria-lakehouse.signalPort" (dict "signal" "logs") }}
*/}}
{{- define "victoria-lakehouse.signalPort" -}}
{{- if eq .signal "traces" -}}10428{{- else -}}9428{{- end -}}
{{- end }}

{{/*
Resolve podSecurityContext: component-specific overrides common.
Usage: {{ include "victoria-lakehouse.podSecurityContext" (dict "component" .Values.logs.select "common" .Values.common) }}
*/}}
{{/*
Opt-in ingest listeners of one component, as a YAML list of {name, port, protocol, arg}.
Off by default: only the insert role serves them, and only when enabled in values.
  logs.insert.syslog.{tcp,udp}.enabled -> -syslog.listenAddr.{tcp,udp} (+ -syslog.tenantID.*)
  traces.insert.otlpGrpc.enabled        -> -otlpGRPCListenAddr (+ TLS flags)
Usage: include "victoria-lakehouse.ingestListeners" (dict "signal" $signal "role" $role "roleVals" $roleVals) | fromYamlArray
*/}}
{{- define "victoria-lakehouse.ingestListeners" -}}
{{- $out := list }}
{{- if eq .role "insert" }}
{{- if eq .signal "logs" }}
{{- range $proto := list "tcp" "udp" }}
{{- $l := dig "syslog" $proto (dict) $.roleVals }}
{{- if $l.enabled }}
{{- $args := list (printf "-syslog.listenAddr.%s=:%v" $proto (default 5140 $l.port | int)) }}
{{- if $l.tenantID }}
{{- $args = append $args (printf "-syslog.tenantID.%s=%s" $proto $l.tenantID) }}
{{- end }}
{{- $defPort := ternary 5140 5141 (eq $proto "tcp") }}
{{- $out = append $out (dict "name" (printf "syslog-%s" $proto) "port" (default $defPort $l.port | int) "protocol" (upper $proto) "args" $args) }}
{{- end }}
{{- end }}
{{- end }}
{{- if eq .signal "traces" }}
{{- $g := dig "otlpGrpc" (dict) .roleVals }}
{{- if $g.enabled }}
{{- $port := default 4317 $g.port | int }}
{{- $args := list (printf "-otlpGRPCListenAddr=:%d" $port) }}
{{- if dig "tls" "enabled" true $g }}
{{- if not (and (dig "tls" "certFile" "" $g) (dig "tls" "keyFile" "" $g)) }}
{{- fail "traces.insert.otlpGrpc.tls.enabled=true needs tls.certFile and tls.keyFile (mount them with traces.insert.extraVolumes/extraVolumeMounts), or set traces.insert.otlpGrpc.tls.enabled=false for a plaintext listener" }}
{{- end }}
{{- $args = append $args (printf "-otlpGRPC.tlsCertFile=%s" (dig "tls" "certFile" "" $g)) }}
{{- $args = append $args (printf "-otlpGRPC.tlsKeyFile=%s" (dig "tls" "keyFile" "" $g)) }}
{{- else }}
{{- $args = append $args "-otlpGRPC.tls=false" }}
{{- end }}
{{- $out = append $out (dict "name" "otlp-grpc" "port" $port "protocol" "TCP" "args" $args) }}
{{- end }}
{{- end }}
{{- end }}
{{- toYaml $out }}
{{- end }}

{{- define "victoria-lakehouse.podSecurityContext" -}}
{{- if .component.podSecurityContext }}
{{- toYaml .component.podSecurityContext }}
{{- else }}
{{- toYaml .common.podSecurityContext }}
{{- end }}
{{- end }}

{{/*
Resolve securityContext: component-specific overrides common.
*/}}
{{- define "victoria-lakehouse.securityContext" -}}
{{- if .component.securityContext }}
{{- toYaml .component.securityContext }}
{{- else }}
{{- toYaml .common.securityContext }}
{{- end }}
{{- end }}

{{/*
Resolve nodeSelector: component-specific overrides common.
*/}}
{{- define "victoria-lakehouse.nodeSelector" -}}
{{- if .component.nodeSelector }}
{{- toYaml .component.nodeSelector }}
{{- else if .common.nodeSelector }}
{{- toYaml .common.nodeSelector }}
{{- end }}
{{- end }}

{{/*
Resolve tolerations: component-specific overrides common.
*/}}
{{- define "victoria-lakehouse.tolerations" -}}
{{- if .component.tolerations }}
{{- toYaml .component.tolerations }}
{{- else if .common.tolerations }}
{{- toYaml .common.tolerations }}
{{- end }}
{{- end }}

{{/*
Resolve affinity: component-specific overrides common.
*/}}
{{- define "victoria-lakehouse.affinity" -}}
{{- if .component.affinity }}
{{- toYaml .component.affinity }}
{{- else if .common.affinity }}
{{- toYaml .common.affinity }}
{{- end }}
{{- end }}

{{/*
Resolve resources: component-specific overrides common.
*/}}
{{- define "victoria-lakehouse.resources" -}}
{{- if .component.resources }}
{{- toYaml .component.resources }}
{{- else if .common.resources }}
{{- toYaml .common.resources }}
{{- end }}
{{- end }}

{{/*
Generic component labels — for non-signal components (vmauth, compaction).
Usage: {{ include "victoria-lakehouse.genericComponentLabels" (dict "root" . "component" "vmauth") }}
*/}}
{{- define "victoria-lakehouse.genericComponentLabels" -}}
{{ include "victoria-lakehouse.labels" .root }}
app.kubernetes.io/component: {{ .component }}
{{- end }}

{{/*
Generic component selector labels — for non-signal components.
Usage: {{ include "victoria-lakehouse.genericComponentSelectorLabels" (dict "root" . "component" "vmauth") }}
*/}}
{{- define "victoria-lakehouse.genericComponentSelectorLabels" -}}
{{ include "victoria-lakehouse.selectorLabels" .root }}
app.kubernetes.io/component: {{ .component }}
{{- end }}
