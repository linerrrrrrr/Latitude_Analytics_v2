"""验证 JQData 在 Windows TUN 环境下选择物理出口。"""

from __future__ import annotations

import importlib
import pathlib
import sys
import unittest
from types import SimpleNamespace
from unittest import mock


project_markers = [".git", ".env", "config/settings.py"]
current_path = pathlib.Path.cwd().resolve()  # 当前工作目录

for candidate_root in [current_path, *current_path.parents]:
    if all((candidate_root / marker).exists() for marker in project_markers):
        sys.path.insert(0, str(candidate_root))
        break
else:
    raise RuntimeError("未找到项目根目录")

jqdata_connection = importlib.import_module("config.jqdata_connection")


class JQDataConnectionTest(unittest.TestCase):
    def test_detect_direct_source_ip_excludes_meta_tun(self) -> None:
        route_output = """
          0.0.0.0          0.0.0.0     192.168.10.1    192.168.10.17     30
          0.0.0.0          0.0.0.0       198.18.0.2       198.18.0.1      0
        """
        route_result = SimpleNamespace(stdout=route_output)

        with (
            mock.patch.object(jqdata_connection.sys, "platform", "win32"),
            mock.patch.object(
                jqdata_connection.subprocess,
                "run",
                return_value=route_result,
            ),
        ):
            source_ip = jqdata_connection.detect_direct_source_ip()

        self.assertEqual(source_ip, "192.168.10.17")

    def test_configure_socket_binds_only_jqdata(self) -> None:
        class FakeSocketHandle:
            def __init__(self) -> None:
                self.bound_address = None

            def bind(self, address: tuple[str, int]) -> None:
                self.bound_address = address

        class FakeTSocket:
            def __init__(self, host: str, port: int) -> None:
                self.host = host
                self.port = port
                self.sock = None

            def _init_sock(self) -> None:
                self.sock = FakeSocketHandle()

        import thriftpy2.rpc as thrift_rpc

        with (
            mock.patch.object(
                jqdata_connection,
                "detect_direct_source_ip",
                return_value="192.168.10.17",
            ),
            mock.patch.object(thrift_rpc, "TSocket", FakeTSocket),
        ):
            source_ip = jqdata_connection.configure_jqdata_direct_socket()
            direct_socket = thrift_rpc.TSocket(
                jqdata_connection.JQDATA_HOST,
                jqdata_connection.JQDATA_PORT,
            )
            direct_socket._init_sock()
            other_socket = thrift_rpc.TSocket("127.0.0.1", 9090)
            other_socket._init_sock()

        self.assertEqual(source_ip, "192.168.10.17")
        self.assertEqual(direct_socket.sock.bound_address, ("192.168.10.17", 0))
        self.assertIsNone(other_socket.sock.bound_address)


if __name__ == "__main__":
    unittest.main()
