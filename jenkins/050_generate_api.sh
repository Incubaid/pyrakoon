#!/bin/bash -xe
echo "Generating API docs"

cd ${WORKSPACE}
source _env/bin/activate

paver epydoc
RESULT=$?

deactivate

mv dist/doc/api _output/

if test $RESULT -eq 0; then true; else false; fi
