#!/bin/bash

IMAGE_NAME=${IMAGE_NAME:=docker.io/prajjawal05/transcoder:latest}
echo $IMAGE_NAME

for a in "$@"; do
    action_name=$(basename $(dirname $a))
    package_name=$(basename $(dirname $(dirname $a)))

    zip_name=${package_name}_${action_name}.zip
    zip -r ${zip_name} $(dirname $a)/*
    wsk action create ${action_name} ${zip_name} --docker ${image_name} --insecure
    rm ${zip_name}
done
