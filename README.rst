Aliyun TableStore SDK for Python
==================================

.. image:: https://travis-ci.org/aliyun/aliyun-tablestore-python-sdk.svg?branch=master
    :target: https://travis-ci.org/aliyun/aliyun-tablestore-python-sdk
.. image:: https://coveralls.io/repos/github/aliyun/aliyun-tablestore-python-sdk/badge.svg?branch=master
    :target: https://coveralls.io/github/aliyun/aliyun-tablestore-python-sdk?branch=master

概述
----

- 此Python SDK基于 `阿里云表格存储服务 <http://www.aliyun.com/product/ots/>`_  API构建。
- 阿里云表格存储是构建在阿里云飞天分布式系统之上的NoSQL数据存储服务，提供海量结构化数据的存储和实时访问。

运行环境
---------

- 安装Python即可运行，目前仅支持python2.7，暂不支持python3

安装方法
---------

PIP安装
--------

.. code-block:: bash

    $ pip install aliyun-tablestore

Github安装
------------

1. 下载源码


.. code-block:: bash

    $ git clone https://github.com/aliyun/aliyun-tablestore-python-sdk.git

2. 安装

.. code-block:: bash

    $ python setup.py install


源码安装
--------

1. 下载SDK发布包并解压
2. 安装


.. code-block:: bash

    $ python setup.py install

示例代码
---------

TBD

执行测试
---------

**注意：测试case中会有清理某个实例下所有表的动作，所以请使用专门的测试实例来测试。**

1. 安装nosetests

.. code-block:: bash

    $ pip install nose

2. 设置执行Case的配置

.. code-block:: bash

    $ export OTS_TEST_ACCESS_KEY_ID=<your access id>
    $ export OTS_TEST_ACCESS_KEY_SECRET=<your access key>
    $ export OTS_TEST_ENDPOINT=<ots service endpoint>
    $ export OTS_TEST_INSTANCE=<your instance name>

2. 运行case

.. code-block:: bash

    $ nosetests tests/

贡献代码
--------
- 我们非常欢迎大家为TableStore Python SDK以及其他TableStore SDK贡献代码

联系我们
--------
- `阿里云TableStore官方网站 <http://www.aliyun.com/product/ots>`_
- `阿里云TableStore官方论坛 <http://bbs.aliyun.com>`_
- `阿里云TableStore官方文档中心 <https://help.aliyun.com/product/8315004_ots.html>`_
- `阿里云云栖社区 <http://yq.aliyun.com>`_
- `阿里云工单系统 <https://workorder.console.aliyun.com/#/ticket/createIndex>`_
