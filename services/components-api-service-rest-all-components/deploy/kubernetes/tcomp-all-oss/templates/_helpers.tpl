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
Define the docker image.
*/}}
{{- define "tcomp-oss-all.image" -}}
{{- $envValues := pluck .Values.global.env .Values | first }}
{{- $imageRepositoryName := default .Values.image.repositoryName $envValues.image.repositoryName -}}
{{- $imageTag := default .Values.image.tag $envValues.image.tag -}}
{{- printf "%s/%s/%s:%s" .Values.global.registry .Values.global.repositoryUser $imageRepositoryName $imageTag }}
{{- end -}}

{{/*
Define the default service name.
*/}}
{{- define "tcomp-oss-all.servicePortName" -}}
{{- $envValues := pluck .Values.global.env .Values | first }}
{{- default .Chart.Name .Values.nameOverride | trunc 10 | printf "%sport" -}}
{{- end -}}

