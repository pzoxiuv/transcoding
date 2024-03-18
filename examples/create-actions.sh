#!/bin/bash

IMAGE_NAME=${IMAGE_NAME:=docker.io/prajjawal05/transcoder:latest}
echo $IMAGE_NAME

for a in "$@"; do
    action_name=$(basename $(dirname $a))
    package_name=$(basename $(dirname $(dirname $a)))
    echo $package_name/$action_name
done

exit

# Copy transcodingActions.py and constants.py to the temporary directory
for action in `find ${action_base_dir}/actions/ -mindepth 1 -type d`; do 
    zip -r ${action}.zip ${action}/*
done
cp -r $(action_dir)/* $
cp transcodingActions.py deployment/__main__.py
cp constants.py deployment/

# Zip the contents of the temporary directory
cd deployment
zip -r action.zip *

# Move the zip file back to the original directory if needed
mv action.zip ../

# Clean up the temporary directory
cd ..
rm action.zip
rm -rf deployment

wsk action create ${action_name} action.zip --docker ${image_name} --insecure
