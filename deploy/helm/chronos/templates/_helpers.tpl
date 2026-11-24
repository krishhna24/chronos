{{/*
Chart name.
*/}}
{{- define "chronos.name" -}}
{{- default .Chart.Name .Values.nameOverride | trunc 63 | trimSuffix "-" }}
{{- end }}

{{/*
Fully qualified name. A release named "chronos" (the documented default)
yields "chronos", the Service name kubectl-at assumes.
*/}}
{{- define "chronos.fullname" -}}
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

{{- define "chronos.chart" -}}
{{- printf "%s-%s" .Chart.Name .Chart.Version | replace "+" "_" | trunc 63 | trimSuffix "-" }}
{{- end }}

{{- define "chronos.selectorLabels" -}}
app.kubernetes.io/name: {{ include "chronos.name" . }}
app.kubernetes.io/instance: {{ .Release.Name }}
{{- end }}

{{- define "chronos.labels" -}}
helm.sh/chart: {{ include "chronos.chart" . }}
{{ include "chronos.selectorLabels" . }}
app.kubernetes.io/version: {{ include "chronos.imageTag" . | quote }}
app.kubernetes.io/managed-by: {{ .Release.Service }}
app.kubernetes.io/part-of: chronos
{{- end }}

{{- define "chronos.imageTag" -}}
{{- default .Chart.AppVersion .Values.image.tag }}
{{- end }}

{{- define "chronos.serviceAccountName" -}}
{{- if .Values.serviceAccount.create }}
{{- default (include "chronos.fullname" .) .Values.serviceAccount.name }}
{{- else }}
{{- default "default" .Values.serviceAccount.name }}
{{- end }}
{{- end }}

{{/*
Cluster-scoped objects are shared by every namespace, so their names carry
the release namespace unless it is already the fullname (the default
"chronos" release in namespace "chronos" gives plain "chronos").
*/}}
{{- define "chronos.clusterName" -}}
{{- $full := include "chronos.fullname" . }}
{{- if eq $full .Release.Namespace }}
{{- $full }}
{{- else }}
{{- printf "%s-%s" .Release.Namespace $full | trunc 63 | trimSuffix "-" }}
{{- end }}
{{- end }}

{{/*
Server arguments generated from .Values.config.
*/}}
{{- define "chronos.args" -}}
{{- $c := .Values.config }}
{{- if not (has $c.storage (list "pebble" "memory")) }}
{{- fail (printf "config.storage must be pebble or memory, got %q" $c.storage) }}
{{- end }}
- --listen=:{{ .Values.containerPort }}
- --storage={{ $c.storage }}
- --data-dir=/var/lib/chronos
- --retention={{ $c.retention }}
- --retention-interval={{ $c.retentionInterval }}
{{- with $c.resources }}
- --resources={{ join "," . }}
{{- end }}
{{- with $c.redactAnnotations }}
- --redact-annotations={{ join "," . }}
{{- end }}
{{- if $c.keepManagedFields }}
- --keep-managed-fields
{{- end }}
- --log-level={{ $c.logLevel }}
- --log-format={{ $c.logFormat }}
- --kube-api-qps={{ $c.kubeApiQPS }}
- --kube-api-burst={{ $c.kubeApiBurst }}
{{- range .Values.extraArgs }}
- {{ . }}
{{- end }}
{{- end }}
