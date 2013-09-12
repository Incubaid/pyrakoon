#!/bin/bash -xe
echo "Running pylint"

cd ${WORKSPACE}
source _env/bin/activate

pylint --rcfile=pylintrc -f parseable --include-ids=y pyrakoon | tee _output/pylint.out
echo "Return code: $?"

deactivate
