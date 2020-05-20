#!/bin/sh

# Exit on error
set -e

echo ""
echo "===================================="
echo "Scenario #4"
echo "===================================="

echo ""
echo "===================================="
echo "Create Iter8 Custom Metric"
echo "===================================="
kubectl apply -n iter8 -f https://raw.githubusercontent.com/iter8-tools/iter8-controller/master/doc/tutorials/istio/bookinfo/iter8_metrics_extended.yaml
kubectl get configmap iter8config-metrics -n iter8 -oyaml

echo ""
echo "===================================="
echo "Create Iter8 Experiment"
echo "===================================="
kubectl apply -n bookinfo-iter8 -f https://raw.githubusercontent.com/iter8-tools/iter8-controller/master/doc/tutorials/istio/bookinfo/canary_reviews-v3_to_reviews-v6.yaml
kubectl get experiments -n bookinfo-iter8

echo ""
echo "===================================="
echo "Deploy canary version"
echo "===================================="
kubectl apply -n bookinfo-iter8 -f https://raw.githubusercontent.com/iter8-tools/iter8-controller/master/doc/tutorials/istio/bookinfo/reviews-v6.yaml
sleep 1
kubectl wait --for=condition=ExperimentCompleted -n bookinfo-iter8 experiments.iter8.tools reviews-v6-rollout --timeout=300s
kubectl get experiments -n bookinfo-iter8

echo ""
echo "===================================="
echo "Test results"
echo "===================================="
kubectl -n bookinfo-iter8 get experiments.iter8.tools reviews-v6-rollout -o yaml
conclusion=`kubectl -n bookinfo-iter8 get experiments.iter8.tools reviews-v6-rollout -o=jsonpath='{.status.assessment.conclusions[0]}'`
if [ "$conclusion" == "All success criteria were  met" ]; then
  echo "Experiment succeeded as expected!"
  exit 1
fi
echo "Experiment failed unexpectedly!"
