#!/usr/bin/env python
# -*-coding:utf-8-*-
'''
Author : ming
date   : 2017/4/24 下午2:54
role   : Version Update
'''
from ots2.protocol import OTSProtocol


def main():
    # OTS_TEST_ACCESS_KEY_ID=LTAI9ayIsTOmzvk5;
    # OTS_TEST_ENDPOINT=http://scormtest.cn-hangzhou.ots.aliyuncs.com;
    # OTS_TEST_INSTANCE=scormtest;
    # OTS_TEST_ACCESS_KEY_SECRET=BOySoSii0izlHy8cOY5jWwXUIiJDTV

    OTS_ID = 'LTAI9ayIsTOmzvk5'
    OTS_SECRET = "BOySoSii0izlHy8cOY5jWwXUIiJDTV"
    OTS_ENDPOINT = "http://scormtest.cn-hangzhou.ots.aliyuncs.com"
    OTS_INSTANCE = "scormtest"

    headers = {'x-ots-apiversion': '2014-08-08', 'x-ots-accesskeyid': 'LTAI9ayIsTOmzvk5',
               'x-ots-date': 'Mon, 24 Apr 2017 06:52:55 GMT', 'x-ots-contentmd5': b'1B2M2Y8AsgTpgAmY7PhCfg==',
               'x-ots-instancename': 'scormtest'}
    query = '/ListTable'
    op = OTSProtocol(OTS_ID, OTS_SECRET, OTS_INSTANCE, 'utf8', None)
    sign = op._make_request_signature(query, headers)
    print(sign)


if __name__ == '__main__':
    # main()
    pass