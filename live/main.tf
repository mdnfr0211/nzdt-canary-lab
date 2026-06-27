resource "kubernetes_namespace" "argocd" {
  metadata {
    name = var.argocd_namespace
  }
}

resource "helm_release" "argo_cd" {
  name             = "argo-cd"
  chart            = "argo-cd"
  repository       = "https://argoproj.github.io/argo-helm"
  version          = var.argocd_chart_version
  namespace        = kubernetes_namespace.argocd.metadata[0].name
  create_namespace = false

  values = [
    yamlencode({
      server = {
        insecure = true
        service = {
          type = "ClusterIP"
        }
      }
      configs = {
        cm = {
          "url"                        = local.git_repo
          "application.instanceLabels" = "app.kubernetes.io/instance"
        }
      }
    })
  ]

  wait            = true
  timeout         = 600
  cleanup_on_fail = true
}