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
Component labels — pass dict with "root" (top-level context) and "component" (string)
Usage: {{ include "victoria-lakehouse.componentLabels" (dict "root" . "component" "select") }}
*/}}
{{- define "victoria-lakehouse.componentLabels" -}}
{{ include "victoria-lakehouse.labels" .root }}
app.kubernetes.io/component: {{ .component }}
{{- end }}

{{/*
Component selector labels
Usage: {{ include "victoria-lakehouse.componentSelectorLabels" (dict "root" . "component" "select") }}
*/}}
{{- define "victoria-lakehouse.componentSelectorLabels" -}}
{{ include "victoria-lakehouse.selectorLabels" .root }}
app.kubernetes.io/component: {{ .component }}
{{- end }}

{{/*
Container image with tag fallback to appVersion
Usage: {{ include "victoria-lakehouse.image" . }}
*/}}
{{- define "victoria-lakehouse.image" -}}
{{- $tag := default .Chart.AppVersion .Values.image.tag -}}
{{- printf "%s:%s" .Values.image.repository $tag -}}
{{- end }}

{{/*
Service port based on mode: 9428 for logs, 10428 for traces
*/}}
{{- define "victoria-lakehouse.servicePort" -}}
{{- if eq (default "logs" .Values.lakehouseConfig.mode) "traces" -}}
10428
{{- else -}}
9428
{{- end -}}
{{- end }}

{{/*
Merge common values into component — returns the merged value for a given key.
Components override common. Empty component values fall through to common.
This is a helper for the templates to resolve per-key.
*/}}

{{/*
Resolve podSecurityContext: component-specific overrides common
Usage: {{ include "victoria-lakehouse.podSecurityContext" (dict "component" .Values.select "common" .Values.common) }}
*/}}
{{- define "victoria-lakehouse.podSecurityContext" -}}
{{- if .component.podSecurityContext }}
{{- toYaml .component.podSecurityContext }}
{{- else }}
{{- toYaml .common.podSecurityContext }}
{{- end }}
{{- end }}

{{/*
Resolve securityContext: component-specific overrides common
*/}}
{{- define "victoria-lakehouse.securityContext" -}}
{{- if .component.securityContext }}
{{- toYaml .component.securityContext }}
{{- else }}
{{- toYaml .common.securityContext }}
{{- end }}
{{- end }}

{{/*
Resolve nodeSelector: component-specific overrides common
*/}}
{{- define "victoria-lakehouse.nodeSelector" -}}
{{- if .component.nodeSelector }}
{{- toYaml .component.nodeSelector }}
{{- else if .common.nodeSelector }}
{{- toYaml .common.nodeSelector }}
{{- end }}
{{- end }}

{{/*
Resolve tolerations: component-specific overrides common
*/}}
{{- define "victoria-lakehouse.tolerations" -}}
{{- if .component.tolerations }}
{{- toYaml .component.tolerations }}
{{- else if .common.tolerations }}
{{- toYaml .common.tolerations }}
{{- end }}
{{- end }}

{{/*
Resolve affinity: component-specific overrides common
*/}}
{{- define "victoria-lakehouse.affinity" -}}
{{- if .component.affinity }}
{{- toYaml .component.affinity }}
{{- else if .common.affinity }}
{{- toYaml .common.affinity }}
{{- end }}
{{- end }}

{{/*
Resolve resources: component-specific overrides common
*/}}
{{- define "victoria-lakehouse.resources" -}}
{{- if .component.resources }}
{{- toYaml .component.resources }}
{{- else if .common.resources }}
{{- toYaml .common.resources }}
{{- end }}
{{- end }}
