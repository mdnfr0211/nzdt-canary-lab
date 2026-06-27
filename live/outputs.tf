output "argocd_admin_password_cmd" {
  description = "Command to fetch the initial ArgoCD admin password."
  value       = "kubectl -n ${var.argocd_namespace} get secret argocd-initial-admin-secret -o jsonpath=\"{.data.password}\" | base64 -d ; echo"
}

output "argocd_port_forward_cmd" {
  description = "Run this in a separate terminal to reach the ArgoCD dashboard locally."
  value       = "kubectl -n ${var.argocd_namespace} port-forward svc/argo-cd-argocd-server 8080:443"
}

output "argocd_dashboard_url" {
  description = "URL to open once the port-forward is running (server.insecure=true means no TLS)."
  value       = "http://localhost:8080"
}

output "rollouts_dashboard_cmd" {
  description = "Port-forward for the Argo Rollouts dashboard (enabled in k8s/argo-rollouts/values.yaml)."
  value       = "kubectl -n argo-rollouts port-forward svc/argo-rollouts-dashboard 3100:3100"
}

output "minikube_tunnel_hint" {
  description = "Run in a separate terminal so the istio-ingressgateway LoadBalancer Service gets a reachable external IP."
  value       = "minikube tunnel"
}

output "istio_ingress_url_cmd" {
  description = "Get the external IP + URL to send load to once `minikube tunnel` is running."
  value       = "kubectl -n istio-system get svc istio-ingressgateway -o jsonpath=\"http://{.status.loadBalancer.ingress[0].ip}:{.spec.ports[?(@.name==\\\"http2\\\")].port}\\n\""
}
