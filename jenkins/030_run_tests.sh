#!/bin/bash -xe
echo "Running tests"

rm -f coverage.xml

cd ${WORKSPACE}
source _env/bin/activate

nosetests --with-coverage --cover-package=pyrakoon --cover-erase --with-doctest --with-xunit --xunit-file=_output/test_results.xml --with-xcoverage pyrakoon test
RESULT=$?

deactivate

test -f coverage.xml && mv coverage.xml _output/

if test $RESULT -eq 0; then true; else false; fi
