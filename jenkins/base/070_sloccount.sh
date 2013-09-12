#!/bin/bash -xue

echo "Installing sloccount"
sudo aptitude install sloccount

echo "Running sloccount"
/usr/bin/sloccount --duplicates --wide --details pyrakoon test >${WORKSPACE}/sloccount.sc
