#kubectl apply -f nfs_vol.yaml && \
#kubectl apply -f nfs.yaml &&  \
kubectl apply -f zookeeper.yml && \
kubectl apply -f kafka-broker.yml && \
kubectl apply -f kafka-service.yml && \
kubectl apply -f pvol.yaml && \
kubectl apply -f pvc.yaml && \
kubectl apply -f deploy_creator1.yaml
#MASTER="$(kubectl get pods -o go-template --template '{{range .items}}{{.metadata.name}}{{"\n"}}{{end}}' --selector 'target=controller')"
#kubectl wait --for=condition=Ready 'pod/'$MASTER
kubectl apply -f svc_create1.yaml
#kubectl cp ./USA-Housing.csv master:app/USA-Housing.csv && \
echo "Setup complete"
