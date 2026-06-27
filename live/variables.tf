variable "gitops_repo_url" {
  type    = string
  default = "https://github.com/mdnfr0211/nzdt-canary-lab.git"
}

variable "gitops_target_revision" {
  type    = string
  default = "main"
}

variable "argocd_chart_version" {
  type    = string
  default = "10.0.0"
}

variable "argocd_namespace" {
  type    = string
  default = "argocd"
}

variable "kubeconfig_path" {
  type    = string
  default = "~/.kube/config"
}

variable "kubernetes_context" {
  type    = string
  default = "minikube"
}

variable "istio_chart_version" {
  type    = string
  default = "1.30.2"
}

variable "argo_rollouts_chart_version" {
  type    = string
  default = "2.41.0"
}

variable "strimzi_chart_version" {
  type    = string
  default = "1.0.0"
}

variable "victoria_metrics_chart_version" {
  type    = string
  default = "0.41.0"
}

variable "opentelemetry_collector_chart_version" {
  type    = string
  default = "0.159.1"
}