#!/bin/bash -xue

echo "Creating virtualenv"

cd ${WORKSPACE}
rm -rf _env
rm -rf _output
mkdir -p _output

virtualenv --no-site-packages _env

source _env/bin/activate
# Dependencies
test -f /tmp/zope.interface-3.6.1.tar.gz || wget -q -O /tmp/zope.interface-3.6.1.tar.gz http://pypi.python.org/packages/source/z/zope.interface/zope.interface-3.6.1.tar.gz
pip install /tmp/zope.interface-3.6.1.tar.gz
test -f /tmp/Twisted-10.2.0.tar.bz2 || wget -q -O /tmp/Twisted-10.2.0.tar.bz2 http://pypi.python.org/packages/source/T/Twisted/Twisted-10.2.0.tar.bz2
pip install /tmp/Twisted-10.2.0.tar.bz2

# Build tool
test -f /tmp/Paver-1.0.3.tar.gz || wget -q -O /tmp/Paver-1.0.3.tar.gz http://pypi.python.org/packages/source/P/Paver/Paver-1.0.3.tar.gz
pip install /tmp/Paver-1.0.3.tar.gz

# Unit tests
test -f /tmp/coverage-3.4b2.tar.gz || wget -q -O /tmp/coverage-3.4b2.tar.gz http://pypi.python.org/packages/source/c/coverage/coverage-3.4b2.tar.gz
pip install /tmp/coverage-3.4b2.tar.gz
test -f /tmp/nose-0.11.4.tar.gz || wget -q -O /tmp/nose-0.11.4.tar.gz http://pypi.python.org/packages/source/n/nose/nose-0.11.4.tar.gz
pip install /tmp/nose-0.11.4.tar.gz
test -f /tmp/nosexcover-1.0.4.tar.gz || wget -q -O /tmp/nosexcover-1.0.4.tar.gz http://pypi.python.org/packages/source/n/nosexcover/nosexcover-1.0.4.tar.gz
pip install /tmp/nosexcover-1.0.4.tar.gz

# Spec tests
test -f /tmp/lettuce-0.1.18.tar.gz || wget -q -O /tmp/lettuce-0.1.18.tar.gz http://pypi.python.org/packages/source/l/lettuce/lettuce-0.1.18.tar.gz
pip install /tmp/lettuce-0.1.18.tar.gz

# Code quality check
test -f /tmp/unittest2-0.5.1.tar.gz || wget -q -O /tmp/unittest2-0.5.1.tar.gz http://pypi.python.org/packages/source/u/unittest2/unittest2-0.5.1.tar.gz
pip install /tmp/unittest2-0.5.1.tar.gz
test -f /tmp/logilab-common-0.53.0.tar.gz || wget -q -O /tmp/logilab-common-0.53.0.tar.gz http://pypi.python.org/packages/source/l/logilab-common/logilab-common-0.53.0.tar.gz
pip install /tmp/logilab-common-0.53.0.tar.gz
test -f /tmp/logilab-astng-0.21.0.tar.gz || wget -q -O /tmp/logilab-astng-0.21.0.tar.gz http://pypi.python.org/packages/source/l/logilab-astng/logilab-astng-0.21.0.tar.gz
pip install /tmp/logilab-astng-0.21.0.tar.gz
test -f /tmp/pylint-0.22.0.tar.gz || wget -q -O /tmp/pylint-0.22.0.tar.gz http://pypi.python.org/packages/source/p/pylint/pylint-0.22.0.tar.gz
pip install /tmp/pylint-0.22.0.tar.gz

# API doc generation
test -f /tmp/docutils-0.7.tar.gz || wget -q -O /tmp/docutils-0.7.tar.gz http://pypi.python.org/packages/source/d/docutils/docutils-0.7.tar.gz
pip install /tmp/docutils-0.7.tar.gz
test -f /tmp/epydoc-3.0.1.tar.gz || wget -q -O /tmp/epydoc-3.0.1.tar.gz http://pypi.python.org/packages/source/e/epydoc/epydoc-3.0.1.tar.gz
pip install /tmp/epydoc-3.0.1.tar.gz

# Fix epydoc ReST markup parser
sed -i "s/child\.data/child/g" `python -c "import os.path; import inspect; import epydoc.markup.restructuredtext; print os.path.abspath(inspect.getfile(epydoc.markup.restructuredtext))" | sed s/\.pyc$/\.py/`
# Remove pyc file
rm -f `python -c "import os.path; import inspect; import epydoc.markup.restructuredtext; print os.path.abspath(inspect.getfile(epydoc.markup.restructuredtext))" | sed s/\.py$/\.pyc/`

# Documentation generation
test -f /tmp/Jinja2-2.5.5.tar.gz || wget -q -O /tmp/Jinja2-2.5.5.tar.gz http://pypi.python.org/packages/source/J/Jinja2/Jinja2-2.5.5.tar.gz
pip install /tmp/Jinja2-2.5.5.tar.gz
test -f /tmp/Pygments-1.3.1.tar.gz || wget -q -O /tmp/Pygments-1.3.1.tar.gz http://pypi.python.org/packages/source/P/Pygments/Pygments-1.3.1.tar.gz
pip install /tmp/Pygments-1.3.1.tar.gz
test -f /tmp/Sphinx-1.0.5.tar.gz || wget -q -O /tmp/Sphinx-1.0.5.tar.gz http://pypi.python.org/packages/source/S/Sphinx/Sphinx-1.0.5.tar.gz
pip install /tmp/Sphinx-1.0.5.tar.gz

deactivate
