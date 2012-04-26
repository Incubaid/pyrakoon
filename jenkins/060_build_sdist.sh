#!/bin/bash -xe
echo "Create distribution"

cd ${WORKSPACE}
source _env/bin/activate

paver sdist
RESULT=$?

deactivate

mv dist/pyrakoon-*.tar.gz _output/

if test $RESULT -eq 0; then true; else false; fi
