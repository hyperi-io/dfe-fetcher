{{/*
Expand the name of the chart.
*/}}
{{- define "dfe-fetcher.name" -}}
{{- default .Chart.Name .Values.nameOverride | trunc 63 | trimSuffix "-" }}
{{- end }}

{{/*
Create a default fully qualified app name.
Truncated at 63 chars because some K8s name fields are limited.
*/}}
{{- define "dfe-fetcher.fullname" -}}
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
Create chart name and version as used by the chart label.
*/}}
{{- define "dfe-fetcher.chart" -}}
{{- printf "%s-%s" .Chart.Name .Chart.Version | replace "+" "_" | trunc 63 | trimSuffix "-" }}
{{- end }}

{{/*
Common labels.
*/}}
{{- define "dfe-fetcher.labels" -}}
helm.sh/chart: {{ include "dfe-fetcher.chart" . }}
{{ include "dfe-fetcher.selectorLabels" . }}
{{- if .Chart.AppVersion }}
app.kubernetes.io/version: {{ .Chart.AppVersion | quote }}
{{- end }}
app.kubernetes.io/managed-by: {{ .Release.Service }}
{{- end }}

{{/*
Selector labels.
*/}}
{{- define "dfe-fetcher.selectorLabels" -}}
app.kubernetes.io/name: {{ include "dfe-fetcher.name" . }}
app.kubernetes.io/instance: {{ .Release.Name }}
{{- end }}

{{/*
Service account name.
*/}}
{{- define "dfe-fetcher.serviceAccountName" -}}
{{- if .Values.serviceAccount.create }}
{{- default (include "dfe-fetcher.fullname" .) .Values.serviceAccount.name }}
{{- else }}
{{- default "default" .Values.serviceAccount.name }}
{{- end }}
{{- end }}

{{/*
kafka secret name — use existing or generate from fullname.
*/}}
{{- define "dfe-fetcher.kafkaSecretName" -}}
{{- if .Values.kafka.existingSecret }}
{{- .Values.kafka.existingSecret }}
{{- else }}
{{- printf "%s-kafka" (include "dfe-fetcher.fullname" .) }}
{{- end }}
{{- end }}

{{/*
aws secret name — use existing or generate from fullname.
*/}}
{{- define "dfe-fetcher.awsSecretName" -}}
{{- if .Values.aws.existingSecret }}
{{- .Values.aws.existingSecret }}
{{- else }}
{{- printf "%s-aws" (include "dfe-fetcher.fullname" .) }}
{{- end }}
{{- end }}

{{/*
azure secret name — use existing or generate from fullname.
*/}}
{{- define "dfe-fetcher.azureSecretName" -}}
{{- if .Values.azure.existingSecret }}
{{- .Values.azure.existingSecret }}
{{- else }}
{{- printf "%s-azure" (include "dfe-fetcher.fullname" .) }}
{{- end }}
{{- end }}

{{/*
m365 secret name — use existing or generate from fullname.
*/}}
{{- define "dfe-fetcher.m365SecretName" -}}
{{- if .Values.m365.existingSecret }}
{{- .Values.m365.existingSecret }}
{{- else }}
{{- printf "%s-m365" (include "dfe-fetcher.fullname" .) }}
{{- end }}
{{- end }}

{{/*
gcp secret name — use existing or generate from fullname.
*/}}
{{- define "dfe-fetcher.gcpSecretName" -}}
{{- if .Values.gcp.existingSecret }}
{{- .Values.gcp.existingSecret }}
{{- else }}
{{- printf "%s-gcp" (include "dfe-fetcher.fullname" .) }}
{{- end }}
{{- end }}
