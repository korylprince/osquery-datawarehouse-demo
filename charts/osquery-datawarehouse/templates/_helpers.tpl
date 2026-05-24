{{- define "osquery-datawarehouse.ingressValues" -}}
{{- $anns := dict -}}
{{- if and .root.Values.ingress.annotations (ne (.root.Values.ingress.annotations | toJson) "{}") -}}
{{- $anns = merge $anns .root.Values.ingress.annotations -}}
{{- end -}}
{{- if and .extraAnnotations (ne (.extraAnnotations | toJson) "{}") -}}
{{- $anns = merge $anns .extraAnnotations -}}
{{- end -}}
ingress:
  enabled: {{ if and .root.Values.ingress.enabled (ne .host "") }}true{{ else }}false{{ end }}
  host: {{ .host | quote }}
  className: {{ .root.Values.ingress.className | quote }}
{{- if ne ($anns | toJson) "{}" }}
  annotations:
{{- toYaml $anns | nindent 4 }}
{{- else }}
  annotations: {}
{{- end }}
  tlsSecretName: {{ .root.Values.ingress.tlsSecretName | quote }}
{{- end }}
