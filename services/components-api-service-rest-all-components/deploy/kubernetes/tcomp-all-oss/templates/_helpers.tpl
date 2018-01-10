{{/* vim: set filetype=mustache: */}}
{{/*
Expand the name of the chart.
*/}}
{{- define "tcomp-oss-all.name" -}}
{{- default .Chart.Name .Values.nameOverride | trunc 63 | trimSuffix "-" -}}
{{- end -}}

{{/*
Create a default fully qualified app name.
Truncate at 63 chars characters due to limitations of the DNS system.
*/}}
{{- define "tcomp-oss-all.fullname" -}}
{{- $name := (include "tcomp-oss-all.name" .) -}}
{{- printf "%s-%s" .Release.Name $name -}}
{{- end -}}

{{/*
Create a default chart name including the version number
*/}}
{{- define "tcomp-oss-all.chart" -}}
{{- $name := (include "tcomp-oss-all.name" .) -}}
{{- printf "%s-%s" $name .Chart.Version | replace "+" "_" -}}
{{- end -}}

{{/*
Define the docker registry key.
*/}}
{{- define "tcomp-oss-all.registryKey" -}}
{{- .Values.global.registryKey | default "talendregistry" }}
{{- end -}}

{{/*
Define labels which are used throughout the chart files
*/}}
{{- define "tcomp-oss-all.labels" -}}
app: {{ include "tcomp-oss-all.fullname" . }}
chart: {{ include "tcomp-oss-all.chart" . }}
release: {{ .Release.Name }}
heritage: {{ .Release.Service }}
{{- end -}}

{{/*
Define the default service port.(must be shorter than 15 chars and must contain only lowercase letters)
*/}}
{{- define "tcomp-oss-all.servicePortName" -}}
{{- "service-port" -}}
{{- end -}}

{{/*
Define the docker registry value
*/}}
{{- define "tcomp-oss-all.imageRegistry" -}}
{{- $envValues := pluck .Values.global.env .Values | first }}
{{- $imageRegistry := default .Values.image $envValues.image | pluck "registry" | first | default .Values.image.registry -}}
{{- if empty $imageRegistry -}}
    {{- "" -}}
{{else}}
   {{- $imageRegistry -}}
{{- end -}}
{{- end -}}

{{/*
Define the docker image.
*/}}
{{- define "tcomp-oss-all.image" -}}
{{- $envValues := pluck .Values.global.env .Values | first }}
{{- $imageRegistry := include "tcomp-oss-all.imageRegistry" . -}}
{{- $imagePath := default .Values.image $envValues.image | pluck "path" | first | default .Values.image.path -}}
{{- if eq $imageRegistry "" -}}
    {{- $imagePath -}}
{{else}}
    {{- printf "%s/%s" $imageRegistry $imagePath -}}
{{- end -}}
{{- end -}}