{{- define "event-platform.name" -}}
{{- default .Chart.Name .Values.nameOverride | trunc 63 | trimSuffix "-" -}}
{{- end -}}

{{- define "event-platform.fullname" -}}
{{- $name := default .Chart.Name .Values.nameOverride -}}
{{- printf "%s-%s" .Release.Name $name | trunc 63 | trimSuffix "-" -}}
{{- end -}}

{{- define "event-platform.namespace" -}}
{{- default "app" .Values.namespace -}}
{{- end -}}

{{- define "event-platform.labels" -}}
app.kubernetes.io/name: {{ include "event-platform.name" . }}
app.kubernetes.io/instance: {{ .Release.Name }}
app.kubernetes.io/managed-by: {{ .Release.Service }}
app.kubernetes.io/part-of: event-platform
helm.sh/chart: {{ printf "%s-%s" .Chart.Name .Chart.Version | replace "+" "_" | trunc 63 | trimSuffix "-" }}
{{- end -}}

{{- define "event-platform.selectorLabels" -}}
app.kubernetes.io/name: {{ include "event-platform.name" . }}
app.kubernetes.io/instance: {{ .Release.Name }}
{{- end -}}

{{- define "event-platform.gatewayImage" -}}
{{- $base := default "" .Values.image.registry -}}
{{- printf "%s%s:%s" $base .Values.image.gateway .Values.image.tag -}}
{{- end -}}

{{- define "event-platform.workerImage" -}}
{{- $base := default "" .Values.image.registry -}}
{{- printf "%s%s:%s" $base .Values.image.worker .Values.image.tag -}}
{{- end -}}

{{- define "event-platform.schemaStrict" -}}
{{- if eq .Values.image.tag "v2" -}}"true"{{- else -}}"false"{{- end -}}
{{- end -}}
