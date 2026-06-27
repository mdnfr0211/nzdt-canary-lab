terraform {
  required_version = ">= 1.5, < 2.0"

  required_providers {
    helm = {
      source  = "hashicorp/helm"
      version = ">= 2.14, < 3.0"
    }
    kubernetes = {
      source  = "hashicorp/kubernetes"
      version = ">= 2.30, < 3.0"
    }
    kubectl = {
      source  = "gavinbunney/kubectl"
      version = ">= 1.18, < 2.0"
    }
  }
  backend "local" {
    path = "terraform.tfstate"
  }
}