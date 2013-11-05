#!/bin/bash -xe

echo "Installing dependencies"
sudo aptitude update
sudo aptitude install -yVDq python-dev libev4 libsnappy1

sudo aptitude install -yVDq \
    pylint \
    python-twisted \
    python-coverage \
    python-nose \
    python-nosexcover

echo "Creating virtualenv"

cd ${WORKSPACE}
rm -rf _env
rm -rf _output
mkdir -p _output

virtualenv _env

source _env/bin/activate
# Dependencies
pip install paver
pip install lettuce
pip install epydoc
pip install sphinx

# Fix epydoc ReST markup parser
sed -i "s/child\.data/child/g" `python -c "import os.path; import inspect; import epydoc.markup.restructuredtext; print os.path.abspath(inspect.getfile(epydoc.markup.restructuredtext))" | sed s/\.pyc$/\.py/`
# Remove pyc file
rm -f `python -c "import os.path; import inspect; import epydoc.markup.restructuredtext; print os.path.abspath(inspect.getfile(epydoc.markup.restructuredtext))" | sed s/\.py$/\.pyc/`

deactivate
