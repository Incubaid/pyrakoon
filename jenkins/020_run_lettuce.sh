#!/bin/bash -xue
echo "Running specs"

cd ${WORKSPACE}
source _env/bin/activate

lettuce -v 3 test/features
RESULT=$?

deactivate

if test $RESULT -eq 0; then true; else false; fi
