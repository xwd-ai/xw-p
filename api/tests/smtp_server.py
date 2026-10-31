"""测试用的进程内 SMTP 服务器。

只实现投递路径需要的命令（EHLO / STARTTLS / AUTH PLAIN|LOGIN / MAIL / RCPT /
DATA / QUIT），把收到的信封与报文留给用例断言。不支持投递、队列与退信，
也不解析邮件内容——这些不是客户端行为的一部分。
"""
from __future__ import annotations

import base64
import socket
import ssl
import threading
import time
from typing import Optional


class FakeSMTPServer:
    """监听在回环地址上的最小 SMTP 服务器。"""

    def __init__(
        self,
        *,
        auth: Optional[tuple[str, str]] = None,
        reject_recipient: str = "",
        advertise_starttls: bool = False,
        tls_context: Optional[ssl.SSLContext] = None,
        silent: bool = False,
    ) -> None:
        self.auth = auth
        self.reject_recipient = reject_recipient
        self.advertise_starttls = advertise_starttls
        self.tls_context = tls_context
        self.silent = silent

        self.messages: list[dict[str, object]] = []
        self.upgrades = 0
        self.accepted = 0

        self._sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self._sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self._sock.bind(("127.0.0.1", 0))
        self._sock.listen(8)
        self._sock.settimeout(0.2)
        self._stop = threading.Event()
        self._thread = threading.Thread(target=self._serve, name="fake-smtp", daemon=True)
        self._lock = threading.Lock()

    # ---- 生命周期 ----

    @property
    def host(self) -> str:
        return "127.0.0.1"

    @property
    def port(self) -> int:
        return self._sock.getsockname()[1]

    def start(self) -> "FakeSMTPServer":
        self._thread.start()
        return self

    def stop(self) -> None:
        self._stop.set()
        try:
            self._sock.close()
        except OSError:
            pass
        self._thread.join(timeout=2.0)

    def __enter__(self) -> "FakeSMTPServer":
        return self.start()

    def __exit__(self, *exc_info: object) -> None:
        self.stop()

    # ---- 接收与对话 ----

    def _serve(self) -> None:
        while not self._stop.is_set():
            try:
                conn, _ = self._sock.accept()
            except (socket.timeout, TimeoutError):
                continue
            except OSError:
                return
            with self._lock:
                self.accepted += 1
            threading.Thread(target=self._handle, args=(conn,), daemon=True).start()

    def _handle(self, conn: socket.socket) -> None:
        current: dict[str, object] = {"to": [], "auth_user": ""}
        try:
            if self.silent:
                # 接受连接但永不说话，用于超时用例。
                time.sleep(1.5)
                return
            stream = conn.makefile("rwb")

            def reply(line: str) -> None:
                stream.write(line.encode() + b"\r\n")
                stream.flush()

            reply("220 fake.test ESMTP ready")
            secure = False
            while True:
                raw = stream.readline()
                if not raw:
                    return
                cmd = raw.decode("utf-8", "replace").rstrip("\r\n")
                upper = cmd.upper()

                if upper.startswith(("EHLO", "HELO")):
                    lines = ["fake.test greets you", "SIZE 10485760", "8BITMIME"]
                    if self.advertise_starttls and not secure:
                        lines.append("STARTTLS")
                    if self.auth and (secure or not self.advertise_starttls):
                        lines.append("AUTH PLAIN LOGIN")
                    for index, item in enumerate(lines):
                        sep = " " if index == len(lines) - 1 else "-"
                        reply(f"250{sep}{item}")

                elif upper == "STARTTLS":
                    if not self.advertise_starttls or self.tls_context is None:
                        reply("502 5.5.1 not implemented")
                        continue
                    reply("220 2.0.0 ready to start TLS")
                    encrypted = self.tls_context.wrap_socket(conn, server_side=True)
                    stream = encrypted.makefile("rwb")
                    secure = True
                    with self._lock:
                        self.upgrades += 1

                elif upper.startswith("AUTH PLAIN"):
                    payload = cmd[len("AUTH PLAIN"):].strip()
                    if not payload:
                        reply("334 ")
                        payload = stream.readline().decode("utf-8", "replace").strip()
                    try:
                        decoded = base64.b64decode(payload).decode("utf-8", "replace")
                    except (ValueError, UnicodeDecodeError):
                        reply("501 5.5.2 cannot decode")
                        continue
                    parts = decoded.split("\x00")
                    if len(parts) < 3:
                        reply("501 5.5.2 malformed")
                        continue
                    user, password = parts[-2], parts[-1]
                    if self.auth is None:
                        reply("503 5.5.1 already authenticated")
                        continue
                    if (user, password) != self.auth:
                        reply("535 5.7.8 authentication credentials invalid")
                        continue
                    current["auth_user"] = user
                    reply("235 2.7.0 authentication successful")

                elif upper.startswith("AUTH LOGIN"):
                    # smtplib 在 PLAIN 被拒后会回退到 LOGIN，两条路径都要能应答。
                    if self.auth is None:
                        reply("503 5.5.1 already authenticated")
                        continue
                    user_b64 = cmd[len("AUTH LOGIN"):].strip()
                    if not user_b64:
                        reply("334 VXNlcm5hbWU6")
                        user_b64 = stream.readline().decode("utf-8", "replace").strip()
                    reply("334 UGFzc3dvcmQ6")
                    pass_b64 = stream.readline().decode("utf-8", "replace").strip()
                    try:
                        user = base64.b64decode(user_b64).decode("utf-8", "replace")
                        password = base64.b64decode(pass_b64).decode("utf-8", "replace")
                    except (ValueError, UnicodeDecodeError):
                        reply("501 5.5.2 cannot decode")
                        continue
                    if (user, password) != self.auth:
                        reply("535 5.7.8 authentication credentials invalid")
                        continue
                    current["auth_user"] = user
                    reply("235 2.7.0 authentication successful")

                elif upper.startswith("MAIL FROM:"):
                    if self.auth and not current["auth_user"]:
                        reply("530 5.7.0 authentication required")
                        continue
                    current["from"] = _envelope_address(cmd)
                    reply("250 2.1.0 ok")

                elif upper.startswith("RCPT TO:"):
                    addr = _envelope_address(cmd)
                    if not addr:
                        reply("501 5.1.3 bad recipient address")
                        continue
                    if self.reject_recipient and addr == self.reject_recipient:
                        reply("550 5.1.1 no such user")
                        continue
                    current["to"].append(addr)  # type: ignore[union-attr]
                    reply("250 2.1.5 ok")

                elif upper == "DATA":
                    if not current["to"]:
                        reply("503 5.5.1 need RCPT first")
                        continue
                    reply("354 end with <CRLF>.<CRLF>")
                    chunks: list[bytes] = []
                    while True:
                        line = stream.readline()
                        if not line:
                            return
                        if line in (b".\r\n", b".\n"):
                            break
                        # 还原点填充：行首两个点还原成一个点。
                        chunks.append(line[1:] if line.startswith(b"..") else line)
                    # 保留原始字节：用 str 解析会让声明为 8bit 的非 ASCII 正文
                    # 被重复转义，测试断言就会看到 \uXXXX 而不是真实文本。
                    current["data"] = b"".join(chunks)
                    reply("250 2.0.0 queued as FAKE123")
                    with self._lock:
                        self.messages.append(current)
                    current = {"to": [], "auth_user": ""}

                elif upper == "RSET":
                    current = {"to": [], "auth_user": ""}
                    reply("250 2.0.0 ok")

                elif upper == "QUIT":
                    reply("221 2.0.0 bye")
                    return

                else:
                    reply("500 5.5.1 unknown command")
        except (OSError, ssl.SSLError):
            return
        finally:
            try:
                conn.close()
            except OSError:
                pass


def _envelope_address(command: str) -> str:
    """从 ``MAIL FROM:<a@b>`` 里取出地址。"""
    start = command.find("<")
    end = command.rfind(">")
    if start < 0 or end <= start:
        return ""
    return command[start + 1 : end].strip()
