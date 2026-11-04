"""可运行的示例：把 SDK 用在一段完整的社区互动流程里。

每个模块都能直接跑（默认连 ``http://127.0.0.1:8000``）::

    cd sdk/python
    python -m examples.post_interaction

用环境变量指向别的部署：``XW_BASE_URL`` / ``XW_API_KEY`` / ``XW_ENVIRONMENT``。

模块把「怎么用 SDK」写成两个入口：``run(client, out)`` 是纯函数式的部分——往
``out`` 打印、返回结果字典，不自己造客户端；``main()`` 只负责从环境变量装配
客户端再调 ``run``。示例因此可以离线断言（测试传入假传输层与 ``StringIO``），
而不必等一个跑着的服务端。
"""
