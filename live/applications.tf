resource "kubectl_manifest" "argocd_istio_base" {
  yaml_body = yamlencode({
    apiVersion = "argoproj.io/v1alpha1"
    kind       = "Application"
    metadata = {
      name      = "istio-base"
      namespace = var.argocd_namespace
      annotations = {
        "argocd.argoproj.io/sync-wave" = "0"
      }
    }
    spec = {
      project = "default"
      sources = [
        {
          repoURL        = "https://istio-release.storage.googleapis.com/charts"
          chart          = "base"
          targetRevision = var.istio_chart_version
          helm = {
            valueFiles = ["$values/k8s/istio/values-base.yaml"]
          }
        },
        {
          repoURL        = local.git_repo
          targetRevision = local.git_branch
          ref            = "values"
        },
      ]
      destination = {
        server    = "https://kubernetes.default.svc"
        namespace = "istio-system"
      }
      syncPolicy = {
        automated = {
          prune    = true
          selfHeal = true
        }
        syncOptions = ["CreateNamespace=true"]
      }
    }
  })

  depends_on = [helm_release.argo_cd]
}

resource "kubectl_manifest" "argocd_istio_istiod" {
  yaml_body = yamlencode({
    apiVersion = "argoproj.io/v1alpha1"
    kind       = "Application"
    metadata = {
      name      = "istio-istiod"
      namespace = var.argocd_namespace
      annotations = {
        "argocd.argoproj.io/sync-wave" = "1"
      }
    }
    spec = {
      project = "default"
      sources = [
        {
          repoURL        = "https://istio-release.storage.googleapis.com/charts"
          chart          = "istiod"
          targetRevision = var.istio_chart_version
          helm = {
            valueFiles = ["$values/k8s/istio/values-istiod.yaml"]
          }
        },
        {
          repoURL        = local.git_repo
          targetRevision = local.git_branch
          ref            = "values"
        },
      ]
      destination = {
        server    = "https://kubernetes.default.svc"
        namespace = "istio-system"
      }
      syncPolicy = {
        automated = {
          prune    = true
          selfHeal = true
        }
        syncOptions = ["CreateNamespace=true"]
      }
    }
  })

  depends_on = [helm_release.argo_cd, kubectl_manifest.argocd_istio_base]
}

resource "kubectl_manifest" "argocd_argo_rollouts" {
  yaml_body = yamlencode({
    apiVersion = "argoproj.io/v1alpha1"
    kind       = "Application"
    metadata = {
      name      = "argo-rollouts"
      namespace = var.argocd_namespace
      annotations = {
        "argocd.argoproj.io/sync-wave" = "1"
      }
    }
    spec = {
      project = "default"
      sources = [
        {
          repoURL        = "https://argoproj.github.io/argo-helm"
          chart          = "argo-rollouts"
          targetRevision = var.argo_rollouts_chart_version
          helm = {
            valueFiles = ["$values/k8s/argo-rollouts/values.yaml"]
          }
        },
        {
          repoURL        = local.git_repo
          targetRevision = local.git_branch
          ref            = "values"
        },
      ]
      destination = {
        server    = "https://kubernetes.default.svc"
        namespace = "argo-rollouts"
      }
      syncPolicy = {
        automated = {
          prune    = true
          selfHeal = true
        }
        syncOptions = ["CreateNamespace=true"]
      }
    }
  })

  depends_on = [helm_release.argo_cd, kubectl_manifest.argocd_istio_base]
}

resource "kubectl_manifest" "argocd_strimzi" {
  yaml_body = yamlencode({
    apiVersion = "argoproj.io/v1alpha1"
    kind       = "Application"
    metadata = {
      name      = "strimzi"
      namespace = var.argocd_namespace
      annotations = {
        "argocd.argoproj.io/sync-wave" = "1"
      }
    }
    spec = {
      project = "default"
      sources = [
        {
          repoURL        = "https://strimzi.io/charts/"
          chart          = "strimzi-kafka-operator"
          targetRevision = var.strimzi_chart_version
          helm = {
            valueFiles = ["$values/k8s/strimzi/values.yaml"]
          }
        },
        {
          repoURL        = local.git_repo
          targetRevision = local.git_branch
          ref            = "values"
        },
      ]
      destination = {
        server    = "https://kubernetes.default.svc"
        namespace = "kafka"
      }
      syncPolicy = {
        automated = {
          prune    = true
          selfHeal = true
        }
        syncOptions = ["CreateNamespace=true"]
      }
    }
  })

  depends_on = [helm_release.argo_cd, kubectl_manifest.argocd_istio_base]
}

resource "kubectl_manifest" "argocd_istio_gateway" {
  yaml_body = yamlencode({
    apiVersion = "argoproj.io/v1alpha1"
    kind       = "Application"
    metadata = {
      name      = "istio-gateway"
      namespace = var.argocd_namespace
      annotations = {
        "argocd.argoproj.io/sync-wave" = "2"
      }
    }
    spec = {
      project = "default"
      sources = [
        {
          repoURL        = "https://istio-release.storage.googleapis.com/charts"
          chart          = "gateway"
          targetRevision = var.istio_chart_version
          helm = {
            valueFiles = ["$values/k8s/istio/values-gateway.yaml"]
          }
        },
        {
          repoURL        = local.git_repo
          targetRevision = local.git_branch
          ref            = "values"
        },
      ]
      destination = {
        server    = "https://kubernetes.default.svc"
        namespace = "istio-system"
      }
      syncPolicy = {
        automated = {
          prune    = true
          selfHeal = true
        }
        syncOptions = ["CreateNamespace=true"]
      }
    }
  })

  depends_on = [
    helm_release.argo_cd,
    kubectl_manifest.argocd_istio_istiod,
    kubectl_manifest.argocd_argo_rollouts,
    kubectl_manifest.argocd_strimzi,
  ]
}

resource "kubectl_manifest" "argocd_strimzi_kafka" {
  yaml_body = yamlencode({
    apiVersion = "argoproj.io/v1alpha1"
    kind       = "Application"
    metadata = {
      name      = "strimzi-kafka"
      namespace = var.argocd_namespace
      annotations = {
        "argocd.argoproj.io/sync-wave" = "2"
      }
    }
    spec = {
      project = "default"
      source = {
        repoURL        = local.git_repo
        targetRevision = local.git_branch
        path           = "k8s/strimzi/manifests"
      }
      destination = {
        server    = "https://kubernetes.default.svc"
        namespace = "kafka"
      }
      syncPolicy = {
        automated = {
          prune    = true
          selfHeal = true
        }
        syncOptions = ["CreateNamespace=true"]
      }
    }
  })

  depends_on = [
    helm_release.argo_cd,
    kubectl_manifest.argocd_strimzi,
  ]
}

resource "kubectl_manifest" "argocd_victoria_metrics" {
  yaml_body = yamlencode({
    apiVersion = "argoproj.io/v1alpha1"
    kind       = "Application"
    metadata = {
      name      = "victoria-metrics"
      namespace = var.argocd_namespace
      annotations = {
        "argocd.argoproj.io/sync-wave" = "2"
      }
    }
    spec = {
      project = "default"
      sources = [
        {
          repoURL        = "https://victoriametrics.github.io/helm-charts/"
          chart          = "victoria-metrics-single"
          targetRevision = var.victoria_metrics_chart_version
          helm = {
            valueFiles = ["$values/k8s/victoria-metrics/values.yaml"]
          }
        },
        {
          repoURL        = local.git_repo
          targetRevision = local.git_branch
          ref            = "values"
        },
      ]
      destination = {
        server    = "https://kubernetes.default.svc"
        namespace = "monitoring"
      }
      syncPolicy = {
        automated = {
          prune    = true
          selfHeal = true
        }
        syncOptions = ["CreateNamespace=true"]
      }
    }
  })

  depends_on = [
    helm_release.argo_cd,
    kubectl_manifest.argocd_istio_istiod,
    kubectl_manifest.argocd_argo_rollouts,
    kubectl_manifest.argocd_strimzi,
  ]
}

resource "kubectl_manifest" "argocd_adot" {
  yaml_body = yamlencode({
    apiVersion = "argoproj.io/v1alpha1"
    kind       = "Application"
    metadata = {
      name      = "adot"
      namespace = var.argocd_namespace
      annotations = {
        "argocd.argoproj.io/sync-wave" = "2"
      }
    }
    spec = {
      project = "default"
      sources = [
        {
          repoURL        = "https://open-telemetry.github.io/opentelemetry-helm-charts"
          chart          = "opentelemetry-collector"
          targetRevision = var.opentelemetry_collector_chart_version
          helm = {
            valueFiles = ["$values/k8s/adot/values.yaml"]
          }
        },
        {
          repoURL        = local.git_repo
          targetRevision = local.git_branch
          ref            = "values"
        },
      ]
      destination = {
        server    = "https://kubernetes.default.svc"
        namespace = "monitoring"
      }
      syncPolicy = {
        automated = {
          prune    = true
          selfHeal = true
        }
        syncOptions = ["CreateNamespace=true"]
      }
    }
  })

  depends_on = [
    helm_release.argo_cd,
    kubectl_manifest.argocd_istio_istiod,
    kubectl_manifest.argocd_argo_rollouts,
    kubectl_manifest.argocd_strimzi,
  ]
}

resource "kubectl_manifest" "argocd_platform" {
  yaml_body = yamlencode({
    apiVersion = "argoproj.io/v1alpha1"
    kind       = "Application"
    metadata = {
      name      = "platform"
      namespace = var.argocd_namespace
      annotations = {
        "argocd.argoproj.io/sync-wave" = "3"
      }
    }
    spec = {
      project = "default"
      source = {
        repoURL        = local.git_repo
        targetRevision = local.git_branch
        path           = "k8s/event-platform"
      }
      destination = {
        server    = "https://kubernetes.default.svc"
        namespace = "app"
      }
      syncPolicy = {
        automated = {
          prune    = true
          selfHeal = true
        }
        syncOptions = ["CreateNamespace=true", "RespectIgnoreDifferences=true"]
      }
      ignoreDifferences = [
        {
          group        = "networking.istio.io"
          kind         = "VirtualService"
          jsonPointers = ["/spec/http/0/route"]
        }
      ]
    }
  })

  depends_on = [
    helm_release.argo_cd,
    kubectl_manifest.argocd_istio_gateway,
    kubectl_manifest.argocd_strimzi_kafka,
    kubectl_manifest.argocd_victoria_metrics,
    kubectl_manifest.argocd_adot,
  ]
}