#!/bin/bash
echo "Distribution Engine Client System"
echo "Performing Magic"
header=0
worker_number=0
while [ "$1" != "" ]; do
    case $1 in
        -f | --file )           shift
                                filename=$1
                                ;;
        -c | --path )           shift
                                program_path=$1
                                ;;
        -z | --hdfs )           hdfs=1
                                ;;
        -r | --remove-headers ) header=1
                                ;;
        #-h | --help )           usage
        #                        exit
        #                        ;;
        #* )                     usage
        #                        exit 1
    esac
    shift
done
##creation of the master and waiting goes here
read -p "Enter K8s Controller External IP : " controller_ip

read -p "Enter split number : " split_number
if [ $split_number -eq 0 ]
then
    read -p "Enter the number of worker nodes" worker_number
fi

controlpod="$(kubectl get pods -o go-template --template '{{range .items}}{{.metadata.name}}{{"\n"}}{{end}}' --selector 'target=controller')"
echo $controlpod
rsync -av --progress --stats -e './rsync_assist.sh' $filename "$controlpod:/dev/shadow/"
#echo curl -d "'""$(generate_post_data)""'" -H \"Content-Type:application/json\" -X POST "http://$controller_ip:4000/api/startdeploy"
#echo "$(curl -d $(generate_post_data) -H \"Content-Type:application/json\" -X POST "http://$controller_ip:4000/api/startdeploy")"

python3 req.py "$controller_ip"':4000' "api/startdeploy" "$filename" "$split_number" "$program_path" "$header" "$worker_number"
