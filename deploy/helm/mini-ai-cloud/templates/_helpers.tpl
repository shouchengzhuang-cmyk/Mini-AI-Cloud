{{- define "mini-ai-cloud.name" -}}
{{- default .Chart.Name .Values.nameOverride | trunc 63 | trimSuffix "-" -}}
{{- end -}}

{{- define "mini-ai-cloud.fullname" -}}
{{- if .Values.fullnameOverride -}}
{{- .Values.fullnameOverride | trunc 63 | trimSuffix "-" -}}
{{- else -}}
{{- $name := include "mini-ai-cloud.name" . -}}
{{- if contains $name .Release.Name -}}
{{- .Release.Name | trunc 63 | trimSuffix "-" -}}
{{- else -}}
{{- printf "%s-%s" .Release.Name $name | trunc 63 | trimSuffix "-" -}}
{{- end -}}
{{- end -}}
{{- end -}}

{{- define "mini-ai-cloud.chart" -}}
{{- printf "%s-%s" .Chart.Name .Chart.Version | replace "+" "_" | trunc 63 | trimSuffix "-" -}}
{{- end -}}

{{- define "mini-ai-cloud.labels" -}}
helm.sh/chart: {{ include "mini-ai-cloud.chart" . }}
app.kubernetes.io/name: {{ include "mini-ai-cloud.name" . }}
app.kubernetes.io/instance: {{ .Release.Name }}
app.kubernetes.io/version: {{ .Chart.AppVersion | quote }}
app.kubernetes.io/managed-by: {{ .Release.Service }}
app.kubernetes.io/part-of: mini-ai-cloud
{{- end -}}

{{- define "mini-ai-cloud.selectorLabels" -}}
app.kubernetes.io/name: {{ include "mini-ai-cloud.name" .root }}
app.kubernetes.io/instance: {{ .root.Release.Name }}
app.kubernetes.io/component: {{ .component }}
{{- end -}}

{{- define "mini-ai-cloud.systemNamespace" -}}
{{- default .Release.Namespace .Values.namespaces.system -}}
{{- end -}}

{{- define "mini-ai-cloud.workloadNamespace" -}}
{{- required "namespaces.workload must name one pre-existing workload namespace" .Values.namespaces.workload -}}
{{- end -}}

{{- define "mini-ai-cloud.image" -}}
{{- if .Values.image.digest -}}
{{- printf "%s@%s" .Values.image.repository .Values.image.digest -}}
{{- else -}}
{{- printf "%s:%s" .Values.image.repository .Values.image.tag -}}
{{- end -}}
{{- end -}}

{{- define "mini-ai-cloud.validate" -}}
{{- if ne (int .Values.controlPlane.replicas) 1 -}}
{{- fail "controlPlane.replicas must be exactly 1; this release has no leader election" -}}
{{- end -}}
{{- if and (eq .Values.service.type "NodePort") (not .Values.global.testMode) -}}
{{- fail "service.type=NodePort is allowed only when global.testMode=true" -}}
{{- end -}}
{{- if and .Values.namespaces.system (ne .Values.namespaces.system .Release.Namespace) -}}
{{- fail "namespaces.system must be empty or match the Helm release namespace" -}}
{{- end -}}
{{- if not .Values.existingSecret.name -}}
{{- fail "existingSecret.name must reference a pre-existing Secret" -}}
{{- end -}}
{{- if and .Values.config.servingFakeEnabled (or (eq .Values.config.appEnvironment "production") (not .Values.config.servingEnabled)) -}}
{{- fail "config.servingFakeEnabled requires servingEnabled=true and appEnvironment=development or test" -}}
{{- end -}}
{{- end -}}

{{- define "mini-ai-cloud.imagePullSecrets" -}}
{{- with .Values.image.pullSecrets }}
imagePullSecrets:
{{- range . }}
  - name: {{ . | quote }}
{{- end }}
{{- end }}
{{- end -}}
