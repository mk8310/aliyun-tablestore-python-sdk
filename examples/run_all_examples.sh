#!/bin/bash

################################################################
################################################################

ots_endpoint=http://ay45w-office.ots.aliyun-inc.com
ots_accessid=bbc07W6jDi214noU
ots_accesskey=Uov2JPb9KADgAaXKnILJQwnVSd0Lhb
ots_instance=wanhong-public

################################################################
################################################################

echo "1. Set env"
export OTS_TEST_ENDPOINT=${ots_endpoint}
export OTS_TEST_ACCESS_KEY_ID=${ots_accessid}
export OTS_TEST_ACCESS_KEY_SECRET=${ots_accesskey}
export OTS_TEST_INSTANCE=${ots_instance}

cd ..
basedir=$(cd "$(dirname "$0")"; pwd)
cd - > /dev/null
export PYTHONPATH=${basedir}

echo "2. Run example"

echo "put run ..."
python2.7 put_row.py
