{{- define "tessera.name" -}}
{{- default .Chart.Name .Values.nameOverride | trunc 63 | trimSuffix "-" -}}
{{- end -}}

{{- define "tessera.fullname" -}}
{{- if .Values.fullnameOverride -}}
{{- .Values.fullnameOverride | trunc 63 | trimSuffix "-" -}}
{{- else -}}
{{- $name := include "tessera.name" . -}}
{{- if contains $name .Release.Name -}}
{{- .Release.Name | trunc 63 | trimSuffix "-" -}}
{{- else -}}
{{- printf "%s-%s" .Release.Name $name | trunc 63 | trimSuffix "-" -}}
{{- end -}}
{{- end -}}
{{- end -}}

{{- define "tessera.chart" -}}
{{- printf "%s-%s" .Chart.Name .Chart.Version | replace "+" "_" | trunc 63 | trimSuffix "-" -}}
{{- end -}}

{{- define "tessera.labels" -}}
helm.sh/chart: {{ include "tessera.chart" . }}
app.kubernetes.io/name: {{ include "tessera.name" . }}
app.kubernetes.io/instance: {{ .Release.Name }}
app.kubernetes.io/version: {{ .Chart.AppVersion | quote }}
app.kubernetes.io/managed-by: {{ .Release.Service }}
{{- end -}}

{{- define "tessera.selectorLabels" -}}
app.kubernetes.io/name: {{ include "tessera.name" . }}
app.kubernetes.io/instance: {{ .Release.Name }}
app.kubernetes.io/component: {{ .component }}
{{- end -}}

{{- define "tessera.workerName" -}}
{{- $prefix := include "tessera.fullname" .root | trunc 31 | trimSuffix "-" -}}
{{- printf "%s-%s" $prefix .worker.name | trunc 63 | trimSuffix "-" -}}
{{- end -}}

{{- define "tessera.image" -}}
{{- printf "%s:%s" .Values.image.repository .Values.image.tag -}}
{{- end -}}

{{- define "tessera.validate" -}}
{{- $names := dict -}}
{{- $ids := dict -}}
{{- $cells := dict -}}
{{- range .Values.workers -}}
{{- if hasKey $names .name -}}
{{- fail (printf "workers contains duplicate name %q" .name) -}}
{{- end -}}
{{- if hasKey $ids .id -}}
{{- fail (printf "workers contains duplicate id %q" .id) -}}
{{- end -}}
{{- $_ := set $names .name true -}}
{{- $_ := set $ids .id true -}}
{{- $workerID := .id -}}
{{- range .cells -}}
{{- $cellKey := printf "%v/%v/%v/%v/%v" .world .cx .cy (.depth | default 0) (.sub | default 0) -}}
{{- if hasKey $cells $cellKey -}}
{{- fail (printf "cell %s is assigned more than once; duplicate worker id %q" $cellKey $workerID) -}}
{{- end -}}
{{- $_ := set $cells $cellKey $workerID -}}
{{- end -}}
{{- end -}}
{{- if and .Values.persistence.enabled (eq .Values.persistence.type "existingClaim") (not .Values.persistence.existingClaim) -}}
{{- fail "persistence.existingClaim is required when persistence.type=existingClaim" -}}
{{- end -}}
{{- end -}}
