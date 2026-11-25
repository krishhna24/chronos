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
Resource types the server can record. Mirrors resources.Builtin in
internal/resources/resources.go; keep the two in sync.
*/}}
{{- define "chronos.builtinTypes" -}}
- {group: "", version: v1, resource: pods, kind: Pod, shortNames: [po]}
- {group: apps, version: v1, resource: deployments, kind: Deployment, shortNames: [deploy]}
- {group: apps, version: v1, resource: replicasets, kind: ReplicaSet, shortNames: [rs]}
- {group: "", version: v1, resource: services, kind: Service, shortNames: [svc]}
- {group: "", version: v1, resource: configmaps, kind: ConfigMap, shortNames: [cm]}
- {group: "", version: v1, resource: events, kind: Event, shortNames: [ev]}
- {group: "", version: v1, resource: secrets, kind: Secret, shortNames: []}
{{- end }}

{{/*
The builtin types selected by .Values.config.resources (empty = all), in
builtin order, as JSON {"types": [...]} with an "id" added to each. Names
resolve like resources.Resolve: case-insensitive canonical ID, plural,
singular, lower-case kind, <plural>.<group>, <kind>.<group>, or short
name. Unknown names fail rendering.
*/}}
{{- define "chronos.selectedTypes" -}}
{{- $builtin := include "chronos.builtinTypes" . | fromYamlArray }}
{{- $want := dict }}
{{- range .Values.config.resources }}
{{- $n := . | toString | trim | lower }}
{{- $found := false }}
{{- range $builtin }}
{{- $id := ternary (printf "%s/%s" .version .resource) (printf "%s/%s/%s" .group .version .resource) (eq .group "") }}
{{- $names := list $id .resource (lower .kind) (trimSuffix "s" .resource) }}
{{- if .group }}
{{- $names = concat $names (list (printf "%s.%s" .resource .group) (printf "%s.%s" (lower .kind) .group)) }}
{{- end }}
{{- $names = concat $names .shortNames }}
{{- if has $n $names }}
{{- $_ := set $want $id true }}
{{- $found = true }}
{{- end }}
{{- end }}
{{- if not $found }}
{{- fail (printf "config.resources: unknown or unsupported resource type %q (supported: pods, deployments, replicasets, services, configmaps, events, secrets)" .) }}
{{- end }}
{{- end }}
{{- $out := list }}
{{- range $builtin }}
{{- $id := ternary (printf "%s/%s" .version .resource) (printf "%s/%s/%s" .group .version .resource) (eq .group "") }}
{{- if or (not $.Values.config.resources) (hasKey $want $id) }}
{{- $out = append $out (set . "id" $id) }}
{{- end }}
{{- end }}
{{- dict "types" $out | toJson }}
{{- end }}

{{/*
Server arguments generated from .Values.config.
*/}}
{{- define "chronos.args" -}}
{{- $c := .Values.config }}
{{- if not (has $c.storage (list "pebble" "memory")) }}
{{- fail (printf "config.storage must be pebble or memory, got %q" $c.storage) }}
{{- end }}
{{- range .Values.extraArgs }}
{{- if or (hasPrefix "--resources" (toString .)) (hasPrefix "-resources" (toString .)) }}
{{- fail "set recorded types with config.resources, not extraArgs: the chart derives RBAC from it" }}
{{- end }}
{{- end }}
- --listen=:{{ .Values.containerPort }}
- --storage={{ $c.storage }}
- --data-dir=/var/lib/chronos
- --retention={{ $c.retention }}
- --retention-interval={{ $c.retentionInterval }}
{{- if $c.resources }}
{{- $ids := list }}
{{- range (include "chronos.selectedTypes" . | fromJson).types }}
{{- $ids = append $ids .id }}
{{- end }}
- --resources={{ join "," $ids }}
{{- end }}
{{- with $c.redactAnnotations }}
- --redact-annotations={{ join "," . }}
{{- end }}
{{- with $c.secretAnnotationAllowlist }}
- --secret-annotation-allowlist={{ join "," . }}
{{- end }}
- --stall-timeout={{ $c.stallTimeout }}
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
