kubectl apply -f nfs_vol.yaml && \
kubectl apply -f nfs.yaml &&  \
kubectl apply -f pvol.yaml && \
kubectl apply -f pvc.yaml && \
kubectl apply -f pod_creator.yaml && \
kubectl wait --for=condition=Ready pod/worker1 pod/worker2 pod/master && \
LEN= $(wc -l USA_Housing.csv)
NUM=2
SPLIT= $((LEN / NUM))
split -d -l $SPLIT source.csv out/source.
for FILE in output*
do
        NAME_LEN=${#FILE}
        NAME_LEN=$(sed -n "s/^.*\.\(\S*\)\..*$/\1/p" $FILE)
        NUMBER_OF_NODE=${$FILE::length}
        
done

echo "Setup complete"