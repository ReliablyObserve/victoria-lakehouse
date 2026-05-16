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
{{- $tag := default .root.Chart.AppVersion .root.Values.image.tag -}}
{{- if eq .signal "traces" -}}
{{- printf "%s:%s" .root.Values.image.traces.repository $tag -}}
{{- else -}}
{{- printf "%s:%s" .root.Values.image.logs.repository $tag -}}
{{- end -}}
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
