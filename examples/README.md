Actions should have the structure `actions/{package name}/{action name}/{source files}`.
The source file containing the action's entry should be named \_\_main\_\_.py

Run `create-actions.py <path of __main__.py>` to create an action using the files in
`dirname <path of __main__.py>`.  You can pass multiple files to create multiple
actions.  E.g.,

    bash create-actions.sh `find actions/ -type f -name __main__.py`
or

    bash create-actions.sh `find actions/basic -type f -name __main__.py`

The default Docker image used for the action is `docker.io/prajjawal05/transcoder:latest`.
Specify a different image by setting the `IMAGE_NAME` variable when running `create-actions.sh`
e.g.,

    IMAGE_NAME=testimage:latest bash create-actions.sh `find actions/ -type f -name __main__.py`
