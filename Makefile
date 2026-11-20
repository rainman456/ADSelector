SHELL := /bin/bash

CLUSTER ?= minikube
RELEASE ?= adselector
NAMESPACE ?= adselector
IMAGE ?= adselector:0.1.0
REPLICAS ?= 2
QPS ?= 200
DURATION ?= 30s
BASE_URL ?= http://$(RELEASE):80

HELM_CHART ?= deploy/helm/adselector
KUBE_CONTEXT = $(if $(filter kind,$(CLUSTER)),kind-$(RELEASE),$(RELEASE))
IMAGE_REPO ?= $(word 1,$(subst :, ,$(IMAGE)))
IMAGE_TAG ?= $(word 2,$(subst :, ,$(IMAGE)))
KUBECTL = kubectl --context $(KUBE_CONTEXT) -n $(NAMESPACE)

.PHONY: cluster-up cluster-down k8s-build k8s-load k8s-deploy k8s-test k8s-up k8s-loadtest k8s-logs

cluster-up:
	@case "$(CLUSTER)" in \
	  kind) \
	    if ! kind get clusters | grep -qx "$(RELEASE)"; then kind create cluster --name "$(RELEASE)"; fi; \
	    kubectl --context "$(KUBE_CONTEXT)" apply -f deploy/k8s/metrics-server-kind.yaml ;; \
	  minikube) \
	    if ! minikube -p "$(RELEASE)" status >/dev/null 2>&1; then minikube start -p "$(RELEASE)"; fi; \
	    kubectl --context "$(KUBE_CONTEXT)" version >/dev/null; \
	    minikube -p "$(RELEASE)" addons enable metrics-server ;; \
	  *) echo "CLUSTER must be kind or minikube" >&2; exit 1 ;; \
	esac

cluster-down:
	@case "$(CLUSTER)" in \
	  kind) kind delete cluster --name "$(RELEASE)" ;; \
	  minikube) minikube -p "$(RELEASE)" delete ;; \
	  *) echo "CLUSTER must be kind or minikube" >&2; exit 1 ;; \
	esac

k8s-build:
	docker build --build-arg VERSION=$(IMAGE_TAG) -t $(IMAGE) .

k8s-load:
	@case "$(CLUSTER)" in \
	  kind) kind load docker-image "$(IMAGE)" --name "$(RELEASE)" ;; \
	  minikube) minikube -p "$(RELEASE)" image load "$(IMAGE)" ;; \
	  *) echo "CLUSTER must be kind or minikube" >&2; exit 1 ;; \
	esac

k8s-deploy:
	helm dependency build $(HELM_CHART)
	helm --kube-context $(KUBE_CONTEXT) upgrade --install $(RELEASE) $(HELM_CHART) \
	  --namespace $(NAMESPACE) --create-namespace \
	  --set image.repository=$(IMAGE_REPO) \
	  --set image.tag=$(IMAGE_TAG) \
	  --wait --timeout 5m

k8s-test:
	$(MAKE) k8s-build IMAGE=$(IMAGE)
	$(MAKE) k8s-load IMAGE=$(IMAGE)
	$(MAKE) k8s-deploy IMAGE=$(IMAGE)
	@deploy=$$(kubectl --context $(KUBE_CONTEXT) -n $(NAMESPACE) get deploy \
	  -l app.kubernetes.io/instance=$(RELEASE),app.kubernetes.io/name=adselector \
	  -o jsonpath='{.items[0].metadata.name}'); \
	  kubectl --context $(KUBE_CONTEXT) -n $(NAMESPACE) rollout status "deployment/$$deploy" --timeout=180s
	@set -euo pipefail; \
	  svc=$$(kubectl --context $(KUBE_CONTEXT) -n $(NAMESPACE) get svc \
	    -l app.kubernetes.io/instance=$(RELEASE),app.kubernetes.io/name=adselector \
	    -o jsonpath='{.items[0].metadata.name}'); \
	  kubectl --context $(KUBE_CONTEXT) -n $(NAMESPACE) port-forward "svc/$$svc" 18080:80 \
	    >/tmp/adselector-port-forward.log 2>&1 & pf=$$!; \
	  trap 'kill $$pf 2>/dev/null || true' EXIT; \
	  for _ in $$(seq 1 60); do \
	    if curl -fsS --max-time 2 http://127.0.0.1:18080/healthz >/dev/null 2>&1; then break; fi; \
	    sleep 1; \
	  done; \
	  curl -fsS --max-time 5 http://127.0.0.1:18080/healthz >/dev/null; \
	  BASE_URL=http://127.0.0.1:18080 bash scripts/k8s-smoke.sh

k8s-up: cluster-up k8s-deploy

k8s-loadtest:
	CLUSTER=$(CLUSTER) KUBE_CONTEXT=$(KUBE_CONTEXT) NAMESPACE=$(NAMESPACE) \
	  RELEASE=$(RELEASE) REPLICAS=$(REPLICAS) QPS=$(QPS) DURATION=$(DURATION) \
	  BASE_URL=$(BASE_URL) bash scripts/k8s-loadtest.sh

k8s-logs:
	kubectl --context $(KUBE_CONTEXT) -n $(NAMESPACE) logs \
	  -l app.kubernetes.io/instance=$(RELEASE),app.kubernetes.io/name=adselector \
	  --tail=100 -f
