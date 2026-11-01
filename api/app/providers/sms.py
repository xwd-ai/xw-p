"""短信渠道的号码规整、计费分段与 console mock 实现。

短信与其它渠道有两处不同，都在这里收口：

1. **号码形态**：网关普遍按 E.164 收号（``+`` 加国家码加号码），而订阅者资料里
   的手机号常带空格、连字符，甚至用括号包住区号。先把号码规整成网关认的形态，
   缺国家码时按配置补上，仍不成立就在进入网关之前判失败——比让网关回一个
   ``21211`` 容易排查得多。
2. **计费分段**：短信按段计费。纯 ASCII 正文走 GSM-7，单段 160 字符，多段拼接
   时每段再让出 7 个字符的拼接头（153）；正文里出现任意非 ASCII 字符（中文、
   emoji）整条转 UCS-2，单段 70、多段 67。执行明细里带上段数，才能解释「一条
   通知为什么按三条计费」。

两个规则与引擎侧 ``internal/pipeline/console_sms.go`` 保持一致：同一份订阅者
资料在两层的判定结果相同，不会出现「引擎说号码不合法、API 说发出去了」。
"""
from __future__ import annotations

import logging

from ..models.channel import Channel
from .base import Delivery, DeliveryResult, Provider

logger = logging.getLogger("xw.provider.sms")

#: E.164 允许的号码位数区间：去掉国家码后的本地号码也不少于 6 位。
PHONE_MIN_DIGITS = 6
PHONE_MAX_DIGITS = 15

#: 号码里常见但无意义的分隔符；规整时直接丢弃。
_SEPARATORS = frozenset(" -\u2010\u2011\u2013\u00a0\t().")

# GSM-7 与 UCS-2 的计费分段参数，见模块 docstring。
_GSM_SINGLE_SEGMENT = 160
_GSM_CONCAT_SEGMENT = 153
_UCS2_SINGLE_SEGMENT = 70
_UCS2_CONCAT_SEGMENT = 67


class PhoneNumberError(ValueError):
    """手机号不合法（为空、含非法字符、位数越界或缺国际区号）。"""


def normalize_phone(raw: object) -> str | None:
    """规整手机号，返回去掉分隔符后的号码；不成立时返回 ``None``。

    允许一个前导 ``+``（写在国际区号之前），其余必须是 ASCII 数字；数字位数
    限定在 6–15 位（E.164 上限 15 位）。``+`` 只认出现在第一个有效字符之前的
    那一个，号码中间的 ``+`` 视为非法输入。
    """
    if raw is None:
        return None
    text = str(raw).strip()
    if not text:
        return None

    kept: list[str] = []
    digits = 0
    for char in text:
        if char == "+" and not kept:
            kept.append(char)
        elif char in _SEPARATORS:
            continue
        elif char.isdigit() and char.isascii():
            kept.append(char)
            digits += 1
        else:
            return None

    if not PHONE_MIN_DIGITS <= digits <= PHONE_MAX_DIGITS:
        return None
    return "".join(kept)


def country_code(raw: object) -> str:
    """校验并返回纯数字的国家码；空值返回空串。

    写成 ``+86``、``86`` 都接受；出现非数字字符直接判配置错误——国家码写错会
    让整批短信发到别人的号段，宁可启动失败也不要带着它跑。
    """
    if raw is None:
        return ""
    text = str(raw).strip()
    if not text:
        return ""
    if text.startswith("+"):
        text = text[1:]
    if not text or not text.isdigit() or not text.isascii():
        raise PhoneNumberError(f"国家码必须是纯数字，收到 {raw!r}")
    if not 1 <= len(text) <= 3:
        raise PhoneNumberError(f"国家码位数不合法（1–3 位），收到 {raw!r}")
    return text


def to_e164(raw: object, default_country_code: str = "") -> str:
    """把号码规整成 E.164；缺国家码且没有缺省值时抛 :class:`PhoneNumberError`。"""
    normalized = normalize_phone(raw)
    if normalized is None:
        raise PhoneNumberError(f"手机号为空或格式非法：{raw!r}")
    if normalized.startswith("+"):
        return normalized

    code = country_code(default_country_code)
    if not code:
        raise PhoneNumberError(
            f"手机号缺少国际区号（{normalized}），请写成 +8613800000000 这样的形式，"
            "或在渠道配置里给出 default_country_code"
        )
    if len(code) + len(normalized) > PHONE_MAX_DIGITS:
        raise PhoneNumberError(f"补上国家码后超过 {PHONE_MAX_DIGITS} 位：+{code}{normalized}")
    return f"+{code}{normalized}"


def sms_segments(body: str) -> int:
    """按 GSM-7 / UCS-2 计费规则计算短信段数；空正文返回 0 段。"""
    if not body:
        return 0

    if all(char.isascii() for char in body):
        length = len(body)
        if length <= _GSM_SINGLE_SEGMENT:
            return 1
        return _ceil_div(length, _GSM_CONCAT_SEGMENT)

    # UCS-2 按 UTF-16 码元计：BMP 之外字符（emoji 等）占两个码元，按字符数
    # 算会把一条本来两段的短信报成一段。
    length = len(body.encode("utf-16-le")) // 2
    if length <= _UCS2_SINGLE_SEGMENT:
        return 1
    return _ceil_div(length, _UCS2_CONCAT_SEGMENT)


def _ceil_div(value: int, divisor: int) -> int:
    return (value + divisor - 1) // divisor


class ConsoleSMSProvider(Provider):
    """sms 渠道的 console mock 实现。

    与通用 :class:`~app.providers.console.ConsoleProvider` 的区别在于它真的校验
    号码格式并计算计费段数：本地跑链路时，一个「13900001111」这样的号码会如实
    报成格式不合法，而不是安静地显示成功——渠道没配凭据和号码填错是两件事，
    混在一起会让演示看起来很顺、上线才发现全失败。
    """

    kind = "console"

    def __init__(
        self,
        channel: Channel = Channel.SMS,
        *,
        identifier: str = "",
        priority: int = 100,
        log: logging.Logger | None = None,
    ) -> None:
        if channel != Channel.SMS:
            raise ValueError("console 短信实现只能用于 sms 渠道")
        super().__init__(channel, identifier=identifier, priority=priority)
        self._log = log or logger

    def _deliver(self, delivery: Delivery) -> DeliveryResult:
        raw = delivery.recipient.address(Channel.SMS) or ""
        to = normalize_phone(raw)
        if to is None:
            self._log.warning(
                "[sms] 投递失败（console mock）provider=%s raw=%r reason=手机号为空或格式非法",
                self._id_for_result,
                raw,
            )
            return DeliveryResult.failed(
                provider_id=self._id_for_result,
                channel=delivery.channel,
                reason="手机号为空或格式非法",
            )

        segments = sms_segments(delivery.body)
        self._log.info(
            "[sms] 投递成功（console mock）provider=%s to=%s segments=%s body=%r",
            self._id_for_result,
            to,
            segments,
            delivery.body,
        )
        return DeliveryResult.ok(self._id_for_result, delivery.channel)


__all__ = [
    "PHONE_MAX_DIGITS",
    "PHONE_MIN_DIGITS",
    "ConsoleSMSProvider",
    "PhoneNumberError",
    "country_code",
    "normalize_phone",
    "sms_segments",
    "to_e164",
]
