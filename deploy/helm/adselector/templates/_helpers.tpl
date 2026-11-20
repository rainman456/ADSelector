{{/*
Common name/label helpers.
*/}}
{{- define "adselector.name" -}}
{{- default .Chart.Name .Values.nameOverride | trunc 63 | trimSuffix "-" -}}
{{- end -}}

{{- define "adselector.fullname" -}}
{{- if .Values.fullnameOverride -}}
{{- .Values.fullnameOverride | trunc 63 | trimSuffix "-" -}}
{{- else -}}
{{- $name := default .Chart.Name .Values.nameOverride -}}
{{- if contains $name .Release.Name -}}
{{- .Release.Name | trunc 63 | trimSuffix "-" -}}
{{- else -}}
{{- printf "%s-%s" .Release.Name $name | trunc 63 | trimSuffix "-" -}}
{{- end -}}
{{- end -}}
{{- end -}}

{{- define "adselector.chart" -}}
{{- printf "%s-%s" .Chart.Name .Chart.Version | replace "+" "_" | trunc 63 | trimSuffix "-" -}}
{{- end -}}

{{- define "adselector.selectorLabels" -}}
app.kubernetes.io/name: {{ include "adselector.name" . }}
app.kubernetes.io/instance: {{ .Release.Name }}
{{- end -}}

{{- define "adselector.labels" -}}
helm.sh/chart: {{ include "adselector.chart" . }}
{{ include "adselector.selectorLabels" . }}
app.kubernetes.io/version: {{ .Chart.AppVersion | quote }}
app.kubernetes.io/managed-by: {{ .Release.Service }}
{{- end -}}

{{- define "adselector.serviceAccountName" -}}
{{- if .Values.serviceAccount.create -}}
{{- default (include "adselector.fullname" .) .Values.serviceAccount.name -}}
{{- else -}}
{{- default "default" .Values.serviceAccount.name -}}
{{- end -}}
{{- end -}}

{{- define "adselector.redisAddr" -}}
{{- if .Values.external.redisAddr -}}
{{- .Values.external.redisAddr -}}
{{- else if .Values.redis.enabled -}}
{{- printf "%s-redis-master:6379" .Release.Name -}}
{{- end -}}
{{- end -}}

{{- define "adselector.kafkaBroker" -}}
{{- if .Values.external.kafkaBroker -}}
{{- .Values.external.kafkaBroker -}}
{{- else if .Values.kafka.enabled -}}
{{- printf "%s-kafka:9093" .Release.Name -}}
{{- end -}}
{{- end -}}
