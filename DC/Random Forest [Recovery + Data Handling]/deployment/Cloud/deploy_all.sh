kubectl apply -f nfs_vol.yaml && \
kubectl apply -f nfs.yaml &&  \
#kubectl apply -f zookeeper.yml
kubectl apply -f pvol.yaml && \
kubectl apply -f pvc.yaml && \
kubectl apply -f deploy_creator1.yaml
kubectl apply -f svc_create1.yaml
kubectl apply -f rabbit.yml
#ZOOK="$(kubectl get pods -o go-template --template '{{range .items}}{{.metadata.name}}{{"\n"}}{{end}}' --selector 'app=zookeeper-1')"
#kubectl wait --for=condition=Ready 'pod/'$ZOOK
#kubectl apply -f kafka-broker.yml
#kubectl apply -f kafka-service.yml
#kubectl cp ./USA-Housing.csv master:app/USA-Housing.csv && \
echo "Setup complete"
