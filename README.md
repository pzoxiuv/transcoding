# transcoding

1. You would need to install ffmpeg first.
2. Run pip3 install -r requirements.txt

codec - coder, decoder (software or hardware algorithm used to compress and decompress digital multimedia data). Transcoding decodes using the original algorithm, and then encodes using a different algorithm. thus changing the codec

1. wsk trigger create transcoder-trigger
2. wsk rule create transcoder-rule transcoder-trigger transcoder
3. wsk trigger fire transcoder-trigger
4. wsk activation list --insecure

5. wsk action delete transcoder
6. wsk action create transcoder --docker docker.io/prajjawal05/transcoder:latest --insecure
7. wsk action invoke --result transcoder --insecure

8. helm repo add minio https://helm.min.io/
9. helm repo update
10. helm install minio minio/minio --namespace openwhisk \
    --set accessKey=access123,secretKey=secret123 \
    --set persistence.enabled=true \
    --set persistence.size=10Gi \
    --set persistence.storageClass=standard

11. wsk property get --auth

12. curl -u 23bc46b1-71f6-4ed5-8c54-816aa4f8c502:123zO3xZCLrMN6v2BKK1dXYFpXlPkccOFqm12CdAsMgRU4VrNZ9lyGVCGuMDGIwP --insecure https://localhost:31001/api/v1/namespaces/whisk.system/packages

13. wsk action create transcoder --docker docker.io/prajjawal05/transcoder:latest main.py --insecure
