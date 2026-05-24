{{- define "cluster-resources.host" -}}
{{- if .hostOverride -}}
{{- .hostOverride -}}
{{- else if .root.Values.global.domainPrefix -}}
{{- printf "%s.%s" .name .root.Values.global.domainPrefix -}}
{{- else -}}
{{- printf "%s.example.com" .name -}}
{{- end -}}
{{- end -}}

{{- define "cluster-resources.ingress.annotations" -}}
{{- if .ingress.annotations -}}
{{ toYaml .ingress.annotations }}
{{- else -}}
{}
{{- end -}}
{{- end -}}

{{- define "cluster-resources.ingress.tls" -}}
{{- if .ingress.tls.enabled -}}
- secretName: {{ default "ingress-tls" .ingress.tls.secretName | quote }}
  hosts:
    - {{ .host | quote }}
{{- else -}}
[]
{{- end -}}
{{- end -}}

{{- define "cluster-resources.certificateDnsNames" -}}
{{- $dnsNames := list -}}
{{- if .rootDomain -}}
{{- $dnsNames = append $dnsNames .rootDomain -}}
{{- $dnsNames = append $dnsNames (printf "*.%s" .rootDomain) -}}
{{- end -}}
{{- if and .domainPrefix (ne .domainPrefix .rootDomain) -}}
{{- $dnsNames = append $dnsNames (printf "*.%s" .domainPrefix) -}}
{{- end -}}
{{- range .extraDnsNames -}}
{{- $dnsNames = append $dnsNames . -}}
{{- end -}}
{{ toYaml ($dnsNames | uniq) }}
{{- end -}}
