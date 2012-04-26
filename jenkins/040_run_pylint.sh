#!/bin/bash -xue
echo "Running pylint"

cd ${WORKSPACE}
source _env/bin/activate

pylint --rcfile=pylintrc -f parseable -i y pyrakoon | tee _output/pylint.out
echo "Return code: $?"

deactivate
