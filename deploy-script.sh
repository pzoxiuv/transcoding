mkdir deployment

# Copy transcodingActions.py and constants.py to the temporary directory
cp transcodingActions.py deployment/__main__.py
cp constants.py deployment/

# Zip the contents of the temporary directory
cd deployment
zip -r action.zip *

# Move the zip file back to the original directory if needed
mv action.zip ../

# Clean up the temporary directory
cd ..
rm -rf deployment

wsk action create transcoder action.zip --docker docker.io/prajjawal05/transcoder:latest --insecure
