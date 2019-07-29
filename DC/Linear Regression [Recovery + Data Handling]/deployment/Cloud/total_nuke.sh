kubectl delete deployments --all
#kubectl delete svc --all
kubectl delete pods --grace-period 0 --force --all
#kubectl delete pvc --all
#kubectl delete pv --all
echo "Cluster Nuked"
