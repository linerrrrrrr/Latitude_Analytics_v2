"""JQData 在 Windows TUN 代理环境下的连接辅助函数。"""

from __future__ import annotations

import ipaddress
import re
import subprocess
import sys
import warnings
from types import ModuleType

from jqdatasdk.client import JQDataClient

JQDATA_HOST = JQDataClient._default_host
JQDATA_PORT = int(JQDataClient._default_port)

TUN_SOURCE_NETWORKS = (ipaddress.ip_network("198.18.0.0/15"),)
DEFAULT_ROUTE_PATTERN = re.compile(
    r"^\s*0\.0\.0\.0\s+0\.0\.0\.0\s+\S+\s+"
    r"(?P<source_ip>\d{1,3}(?:\.\d{1,3}){3})\s+(?P<metric>\d+)\s*$",
    re.MULTILINE,
)

# jqdatasdk 仍使用 NumPy 2.4 已弃用的 dtype(align=0) 调用；该警告会在每次
# RPC 反序列化时重复出现，与返回数据或拉取状态无关，只屏蔽这一条精确来源。
warnings.filterwarnings(
    "ignore",
    message=r"dtype\(\): align should be passed as Python or NumPy boolean.*",
    module=r"jqdatasdk\.compat\.pickle_compat",
)


def detect_direct_source_ip() -> str | None:
    """返回 Windows 物理默认路由的源地址，排除 Meta TUN 地址段。"""
    if sys.platform != "win32":
        return None

    try:
        route_result = subprocess.run(
            ["route", "print", "0.0.0.0"],
            check=True,
            capture_output=True,
            text=True,
            errors="replace",
        )
    except (OSError, subprocess.CalledProcessError):
        return None

    candidates: list[tuple[int, str]] = []
    for match in DEFAULT_ROUTE_PATTERN.finditer(route_result.stdout):
        source_ip = ipaddress.ip_address(match.group("source_ip"))
        if source_ip.is_unspecified or source_ip.is_loopback or source_ip.is_link_local:
            continue
        if any(source_ip in network for network in TUN_SOURCE_NETWORKS):
            continue
        candidates.append((int(match.group("metric")), str(source_ip)))

    return min(candidates)[1] if candidates else None


def configure_jqdata_direct_socket() -> str | None:
    """只让 JQData RPC 套接字绑定物理出口，其他流量继续使用代理。"""
    source_ip = detect_direct_source_ip()
    if source_ip is None:
        return None

    import thriftpy2.rpc as thrift_rpc

    original_socket_class = getattr(
        thrift_rpc.TSocket,
        "_jqdata_original_socket_class",
        thrift_rpc.TSocket,
    )
    if getattr(thrift_rpc.TSocket, "_jqdata_source_ip", None) == source_ip:
        return source_ip

    class DirectJQDataSocket(original_socket_class):
        _jqdata_original_socket_class = original_socket_class
        _jqdata_source_ip = source_ip

        def _init_sock(self) -> None:
            super()._init_sock()
            if self.host == JQDATA_HOST and int(self.port) == JQDATA_PORT:
                self.sock.bind((source_ip, 0))

    thrift_rpc.TSocket = DirectJQDataSocket
    return source_ip


def authenticate_jqdata(username: str, password: str) -> ModuleType:
    """配置直连套接字、完成身份认证，并返回可直接使用的 JQData SDK。"""
    configure_jqdata_direct_socket()

    import jqdatasdk

    jqdatasdk.auth(username, password)
    return jqdatasdk
