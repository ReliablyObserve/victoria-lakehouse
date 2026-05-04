{{/*
Expand the name of the chart.
*/}}
{{- define "victoria-lakehouse.name" -}}
{{- default .Chart.Name .Values.nameOverride | trunc 63 | trimSuffix "-" }}
{{- end }}

{{/*
Create a default fully qualified app name.
We truncate at 63 chars because some Kubernetes name fields are limited to this.
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
Chart label value (name-version).
*/}}
{{- define "victoria-lakehouse.chart" -}}
{{- printf "%s-%s" .Chart.Name .Chart.Version | replace "+" "_" | trunc 63 | trimSuffix "-" }}
{{- end }}

{{/*
Common labels shared by all resources.
*/}}
{{- define "victoria-lakehouse.labels" -}}
helm.sh/chart: {{ include "victoria-lakehouse.chart" . }}
app.kubernetes.io/version: {{ .Chart.AppVersion | quote }}
app.kubernetes.io/managed-by: {{ .Release.Service }}
{{- with .Values.global.commonLabels }}
{{ toYaml . }}
{{- end }}
{{- end }}

{{/*
Component-specific labels. Call with a dict containing "context" (the root .)
and "component" (the component name string).
Usage: {{ include "victoria-lakehouse.componentLabels" (dict "context" . "component" "select") }}
*/}}
{{- define "victoria-lakehouse.componentLabels" -}}
{{ include "victoria-lakehouse.labels" .context }}
{{ include "victoria-lakehouse.componentSelectorLabels" (dict "context" .context "component" .component) }}
{{- end }}

{{/*
Selector labels for a specific component.
Usage: {{ include "victoria-lakehouse.componentSelectorLabels" (dict "context" . "component" "select") }}
*/}}
{{- define "victoria-lakehouse.componentSelectorLabels" -}}
app.kubernetes.io/name: {{ include "victoria-lakehouse.name" .context }}
app.kubernetes.io/instance: {{ .context.Release.Name }}
app.kubernetes.io/component: {{ .component }}
{{- end }}

{{/*
Resolve the port number based on mode and optional override.
Usage: {{ include "victoria-lakehouse.port" (dict "context" . "servicePort" .Values.select.service.port) }}
*/}}
{{- define "victoria-lakehouse.port" -}}
{{- if .servicePort }}
{{- .servicePort }}
{{- else if eq .context.Values.mode "traces" }}
{{- 10428 }}
{{- else }}
{{- 9428 }}
{{- end }}
{{- end }}

{{/*
Render the container image reference.
Accepts a dict with "context" (root .) and "imageOverride" (component-level image override).
Falls back to global image settings.
Usage: {{ include "victoria-lakehouse.image" (dict "context" . "imageOverride" .Values.select.image) }}
*/}}
{{- define "victoria-lakehouse.image" -}}
{{- $repo := .context.Values.image.repository }}
{{- $tag := .context.Values.image.tag | default .context.Chart.AppVersion }}
{{- $pullPolicy := .context.Values.image.pullPolicy }}
{{- if .imageOverride }}
  {{- if .imageOverride.repository }}
    {{- $repo = .imageOverride.repository }}
  {{- end }}
  {{- if .imageOverride.tag }}
    {{- $tag = .imageOverride.tag }}
  {{- end }}
  {{- if .imageOverride.pullPolicy }}
    {{- $pullPolicy = .imageOverride.pullPolicy }}
  {{- end }}
{{- end }}
{{- printf "%s:%s" $repo $tag }}
{{- end }}

{{/*
Render image pull policy from override or global.
*/}}
{{- define "victoria-lakehouse.imagePullPolicy" -}}
{{- $pullPolicy := .context.Values.image.pullPolicy }}
{{- if .imageOverride }}
  {{- if .imageOverride.pullPolicy }}
    {{- $pullPolicy = .imageOverride.pullPolicy }}
  {{- end }}
{{- end }}
{{- $pullPolicy }}
{{- end }}

{{/*
Pod security context: uses component override if set, otherwise defaultSecurityContext.pod.
Usage: {{ include "victoria-lakehouse.podSecurityContext" (dict "context" . "override" .Values.select.podSecurityContext) }}
*/}}
{{- define "victoria-lakehouse.podSecurityContext" -}}
{{- if .override }}
{{- toYaml .override }}
{{- else }}
{{- toYaml .context.Values.defaultSecurityContext.pod }}
{{- end }}
{{- end }}

{{/*
Container security context: uses component override if set, otherwise defaultSecurityContext.container.
Usage: {{ include "victoria-lakehouse.containerSecurityContext" (dict "context" . "override" .Values.select.securityContext) }}
*/}}
{{- define "victoria-lakehouse.containerSecurityContext" -}}
{{- if .override }}
{{- toYaml .override }}
{{- else }}
{{- toYaml .context.Values.defaultSecurityContext.container }}
{{- end }}
{{- end }}

{{/*
Service account name for a component.
Usage: {{ include "victoria-lakehouse.serviceAccountName" (dict "context" . "component" "select" "sa" .Values.select.serviceAccount) }}
*/}}
{{- define "victoria-lakehouse.serviceAccountName" -}}
{{- if .sa.name }}
{{- .sa.name }}
{{- else }}
{{- printf "%s-%s" (include "victoria-lakehouse.fullname" .context) .component }}
{{- end }}
{{- end }}

{{/*
Common S3 args shared by both insert and select.
Usage: {{ include "victoria-lakehouse.s3Args" . }}
*/}}
{{- define "victoria-lakehouse.s3Args" -}}
- "--lakehouse.s3.bucket={{ .Values.s3.bucket }}"
- "--lakehouse.s3.region={{ .Values.s3.region }}"
{{- if .Values.s3.prefix }}
- "--lakehouse.s3.prefix={{ .Values.s3.prefix }}"
{{- end }}
{{- if .Values.s3.endpoint }}
- "--lakehouse.s3.endpoint={{ .Values.s3.endpoint }}"
{{- end }}
{{- if .Values.s3.accessKey }}
- "--lakehouse.s3.access-key={{ .Values.s3.accessKey }}"
{{- end }}
{{- if .Values.s3.secretKey }}
- "--lakehouse.s3.secret-key={{ .Values.s3.secretKey }}"
{{- end }}
{{- if .Values.s3.forcePathStyle }}
- "--lakehouse.s3.force-path-style=true"
{{- end }}
- "--lakehouse.s3.max-connections={{ .Values.s3.maxConnections }}"
- "--lakehouse.s3.timeout={{ .Values.s3.timeout }}"
{{- end }}

{{/*
Common discovery args shared by both insert and select.
Usage: {{ include "victoria-lakehouse.discoveryArgs" . }}
*/}}
{{- define "victoria-lakehouse.discoveryArgs" -}}
{{- if .Values.discovery.headlessService }}
- "--lakehouse.discovery.headless-service={{ .Values.discovery.headlessService }}"
{{- end }}
{{- if .Values.discovery.storageNodes }}
- "--lakehouse.discovery.storage-nodes={{ join "," .Values.discovery.storageNodes }}"
{{- end }}
{{- if .Values.discovery.partitionAuthKey }}
- "--lakehouse.discovery.partition-auth-key={{ .Values.discovery.partitionAuthKey }}"
{{- end }}
{{- if .Values.discovery.peerHeadlessService }}
- "--lakehouse.discovery.peer-headless-service={{ .Values.discovery.peerHeadlessService }}"
{{- end }}
- "--lakehouse.discovery.refresh-interval={{ .Values.discovery.refreshInterval }}"
- "--lakehouse.discovery.timeout={{ .Values.discovery.timeout }}"
{{- end }}

{{/*
Common cache, peer, manifest, query, startup args.
Usage: {{ include "victoria-lakehouse.commonArgs" . }}
*/}}
{{- define "victoria-lakehouse.commonArgs" -}}
- "--lakehouse.cache.memory-limit={{ .Values.cache.memoryLimit }}"
- "--lakehouse.cache.eviction-watermark={{ .Values.cache.evictionWatermark }}"
{{- if .Values.peer.authKey }}
- "--lakehouse.peer.auth-key={{ .Values.peer.authKey }}"
{{- end }}
- "--lakehouse.peer.timeout={{ .Values.peer.timeout }}"
- "--lakehouse.peer.max-connections={{ .Values.peer.maxConnections }}"
{{- if .Values.hotBoundary }}
- "--lakehouse.hot-boundary={{ .Values.hotBoundary }}"
{{- end }}
- "--lakehouse.manifest.refresh-interval={{ .Values.manifest.refreshInterval }}"
- "--lakehouse.manifest.persist-path={{ .Values.manifest.persistPath }}"
{{- if .Values.manifest.sqsQueueUrl }}
- "--lakehouse.manifest.sqs-queue-url={{ .Values.manifest.sqsQueueUrl }}"
{{- end }}
- "--lakehouse.query.max-concurrent={{ .Values.query.maxConcurrent }}"
- "--lakehouse.query.timeout={{ .Values.query.timeout }}"
- "--lakehouse.query.slow-threshold={{ .Values.query.slowThreshold }}"
{{- if .Values.startup.serveStale }}
- "--lakehouse.startup.serve-stale=true"
{{- end }}
- "--lakehouse.startup.warmup-window={{ .Values.startup.warmupWindow }}"
- "--lakehouse.startup.max-warmup-time={{ .Values.startup.maxWarmupTime }}"
{{- range .Values.schema.extraPromoted }}
- "--lakehouse.schema.extra-promoted={{ .name }}:{{ .type }}{{ if .bloom }}:bloom{{ end }}"
{{- end }}
{{- end }}

{{/*
Global annotations including common annotations.
Usage: {{ include "victoria-lakehouse.globalAnnotations" . }}
*/}}
{{- define "victoria-lakehouse.globalAnnotations" -}}
{{- with .Values.global.commonAnnotations }}
{{ toYaml . }}
{{- end }}
{{- end }}
