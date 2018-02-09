{{/* vim: set filetype=mustache: */}}
{{/*
Expand the name of the chart.
*/}}
{{- define "tcomposs.name" -}}
{{- default .Chart.Name .Values.nameOverride | trunc 63 | trimSuffix "-" -}}
{{- end -}}

{{/*
Create a default fully qualified app name.
Truncate at 63 chars characters due to limitations of the DNS system.
*/}}
{{- define "tcomposs.fullname" -}}
{{- $name := (include "tcomposs.name" .) -}}
{{- printf "%s-%s" .Release.Name $name -}}
{{- end -}}

{{/*
Create a default chart name including the version number
*/}}
{{- define "tcomposs.chart" -}}
{{- $name := (include "tcomposs.name" .) -}}
{{- printf "%s-%s" $name .Chart.Version | replace "+" "_" -}}
{{- end -}}

{{/*
Define the docker registry key.
*/}}
{{- define "tcomposs.registryKey" -}}
{{- .Values.global.registryKey | default "talendregistry" }}
{{- end -}}

{{/*
Define labels which are used throughout the chart files
*/}}
{{- define "tcomposs.labels" -}}
app: {{ include "tcomposs.fullname" . }}
chart: {{ include "tcomposs.chart" . }}
release: {{ .Release.Name }}
heritage: {{ .Release.Service }}
{{- end -}}

{{/*
Define the docker image.
*/}}
{{- define "tcomposs.image" -}}
{{- if eq (default "" .Values.image.registry) "" -}}
    {{- printf "%s:%s" .Values.image.path (default .Values.global.tcompVersion .Values.image.tag | default "latest" ) -}}
{{else}}
    {{- printf "%s/%s:%s" .Values.image.registry .Values.image.path (default .Values.global.tcompVersion .Values.image.tag | default "latest" ) -}}
{{- end -}}
{{- end -}}

{{/*
Create a default fully qualified app name.
*/}}
{{- define "tcomposs.service.name" -}}
{{- $name := .Values.service.name | trunc 63 | trimSuffix "-" -}}
{{- printf "%s-%s" .Release.Name $name -}}
{{- end -}}