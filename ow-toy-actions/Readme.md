# Setting up openwhisk

1. sh start-kind.sh
2. helm repo add openwhisk https://openwhisk.apache.org/charts
3. helm repo update
4. helm install owdev openwhisk/openwhisk -n openwhisk --create-namespace -f mycluster.yaml
