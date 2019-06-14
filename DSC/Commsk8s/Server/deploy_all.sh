kubectl apply -f nfs_vol.yaml && \
kubectl apply -f nfs.yaml &&  \
kubectl apply -f pvol.yaml && \
kubectl apply -f pvc.yaml && \
kubectl apply -f pod_creator.yaml && \
kubectl wait --for=condition=Ready pod/worker1 pod/worker2 pod/worker3 && \
echo "Setup complete"
