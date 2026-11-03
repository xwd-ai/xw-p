"""订阅者联系方式的规整与格式校验。

订阅者档案里有两类地址直接决定「能不能投出去」，也同时是唯一性判定的键：

- **邮箱**：同一个邮箱绑在两个订阅者身上，一条点赞通知就会投两遍，退信也
  归不到人；
- **手机号**：网关按 E.164 收号，档案里却常写成本地格式（``13800000000``）
  或用空格、连字符、括号分隔的形态，同一号码的几种写法在查重时应当收敛成
  一个。

因此这里做两件事，并且放在同一处，避免接口、存储、投递路径各写一份：

1. **规整**：邮箱统一转小写后存储（现实中的邮箱服务商都不区分大小写，
   保留大小写只会让同一个地址在库里出现两次）；手机号规整成 E.164，缺区号
   时按 ``XW_SUBSCRIBER_COUNTRY_CODE`` 补齐，没配就如实报错而不猜。
2. **校验**：邮箱按 RFC 5321/5322 的常见子集校验（长度、用户名可用字符、
   域名段落、顶级域名），手机号按 :mod:`app.providers.sms` 的同一套规则
   校验——投递侧认什么，建档侧就收什么。

唯一性判定用 :func:`address_key` 算出的比较键，与用户怎么写无关。
"""
from __future__ import annotations

import re

from . import config
from .providers.sms import PhoneNumberError, normalize_phone, to_e164

#: 邮箱总长上限（RFC 5321 的路径长度限制），以及用户名与域名的分段上限。
EMAIL_MAX_LENGTH = 254
EMAIL_LOCAL_MAX = 64
DOMAIN_MAX_LENGTH = 253
DOMAIN_LABEL_MAX = 63

#: 用户名部分允许的字符：atext 与「点」（RFC 5322 3.4.1）。
#: 这里不收引号包裹的写法（``"a b"@example.com``）——它在真实信箱里几乎不出现，
#: 收进来只会让后续的模板渲染与 URL 拼接多一种要转义的情形。
_LOCAL_CHARS = frozenset("abcdefghijklmnopqrstuvwxyz0123456789!#$%&'*+-/=?^_`{|}~.")

#: 域名段落：字母数字与连字符，首尾不能是连字符。
_LABEL_RE = re.compile(r"^[a-z0-9](?:[a-z0-9-]*[a-z0-9])?$")

#: 顶级域名：纯字母（通用与国别域名），或 IDN 的 punycode 写法。
_TLD_RE = re.compile(r"^(?:[a-z]{2,63}|xn--[a-z0-9][a-z0-9-]{0,57})$")

#: 唯一性判定支持的字段。
ADDRESS_FIELDS = ("email", "phone")

_FIELD_LABELS = {"email": "邮箱", "phone": "手机号"}


class AddressFormatError(ValueError):
    """地址格式非法（空值、字符越界、长度越界、缺区号等）。

    带 ``field`` 让调用方知道是哪一项出错；``reason`` 是给人看的话，
    要能指出下一步怎么改，而不是只说「格式不合法」。
    """

    def __init__(self, field: str, reason: str) -> None:
        self.field = field
        self.reason = reason
        super().__init__(f"{_FIELD_LABELS.get(field, field)}：{reason}")


def normalize_email(raw: object) -> str:
    """校验并返回小写形态的邮箱地址；不合法时抛 :class:`AddressFormatError`。"""
    text = str(raw).strip() if raw is not None else ""
    if not text:
        raise AddressFormatError("email", "不能为空")
    if len(text) > EMAIL_MAX_LENGTH:
        raise AddressFormatError("email", f"长度不能超过 {EMAIL_MAX_LENGTH} 个字符")
    if any(char.isspace() for char in text):
        raise AddressFormatError("email", "不能包含空白字符")
    if text.count("@") != 1:
        raise AddressFormatError("email", f"必须恰好有一个 @：{text}")

    local, domain = text.split("@")
    if not local:
        raise AddressFormatError("email", "缺少 @ 前面的用户名")
    if not domain:
        raise AddressFormatError("email", "缺少 @ 后面的域名")
    if len(local) > EMAIL_LOCAL_MAX:
        raise AddressFormatError("email", f"@ 前面的用户名不能超过 {EMAIL_LOCAL_MAX} 个字符")
    if local.startswith(".") or local.endswith(".") or ".." in local:
        raise AddressFormatError("email", "用户名部分不能以点开头或结尾，也不能出现连续的点")

    lowered_local = local.lower()
    bad = sorted({char for char in lowered_local if char not in _LOCAL_CHARS})
    if bad:
        raise AddressFormatError(
            "email", f"用户名部分包含不允许的字符：{''.join(bad)}"
        )
    if not domain.isascii():
        # 国际化域名要先转成 punycode：不同客户端对 Unicode 域名的规范化结果
        # 不一致，直接收下会让「同一个域名」在查重时分成两个。
        raise AddressFormatError("email", "域名部分请改用 punycode 写法（xn--…）")

    domain = domain.lower()
    if len(domain) > DOMAIN_MAX_LENGTH:
        raise AddressFormatError("email", f"域名不能超过 {DOMAIN_MAX_LENGTH} 个字符")

    labels = domain.split(".")
    if len(labels) < 2:
        raise AddressFormatError("email", f"域名缺少顶级域名：{domain}")
    for label in labels:
        if not label:
            raise AddressFormatError("email", "域名里不能出现连续的点")
        if len(label) > DOMAIN_LABEL_MAX:
            raise AddressFormatError("email", f"域名段落不能超过 {DOMAIN_LABEL_MAX} 个字符")
        if not _LABEL_RE.match(label):
            raise AddressFormatError(
                "email",
                f"域名段落不合法：{label}（只能是字母、数字与连字符，且首尾不能是连字符）",
            )
    if not _TLD_RE.match(labels[-1]):
        raise AddressFormatError("email", f"顶级域名不合法：{labels[-1]}")

    return f"{lowered_local}@{domain}"


def normalize_phone_address(
    raw: object, default_country_code: str | None = None
) -> str:
    """校验并返回 E.164 形态的手机号；不合法时抛 :class:`AddressFormatError`。

    ``default_country_code`` 留空表示取配置里的 ``XW_SUBSCRIBER_COUNTRY_CODE``；
    两者都为空时只接受自带 ``+`` 区号的号码——按位数猜国家码会把号码发到别人
    的号段上，这类错误在投递时才暴露，代价是一次真实的误发。
    """
    answer = config.SUBSCRIBER_COUNTRY_CODE if default_country_code is None else default_country_code
    text = str(raw).strip() if raw is not None else ""
    if not text:
        raise AddressFormatError("phone", "不能为空")

    normalized = normalize_phone(text)
    if normalized is None:
        raise AddressFormatError(
            "phone",
            "格式非法：只允许数字、一个前导 + 与空格/连字符/括号这类分隔符，位数 6–15",
        )
    if normalized.startswith("+"):
        return normalized
    if not answer:
        raise AddressFormatError(
            "phone",
            f"缺少国际区号（{normalized}）：请写成 +8613800000000 这样的形式，"
            "或设置 XW_SUBSCRIBER_COUNTRY_CODE",
        )
    try:
        return to_e164(normalized, answer)
    except PhoneNumberError as exc:
        raise AddressFormatError("phone", str(exc)) from exc


def normalize_address(field: str, raw: object) -> str | None:
    """按字段名规整地址；空值返回 ``None``（表示「未绑定」）。"""
    if raw is None:
        return None
    text = str(raw).strip()
    if not text:
        return None
    if field == "email":
        return normalize_email(text)
    if field == "phone":
        return normalize_phone_address(text)
    raise ValueError(f"不支持的地址字段：{field}")


def address_key(field: str, raw: object) -> str:
    """返回唯一性判定用的比较键。

    邮箱取小写，手机号取去分隔符后的号码（``+86 138 0000 0000`` 与
    ``+8613800000000`` 是同一个号）。库里可能存在早于校验规则的记录，
    规整不了就退回裁剪后的原文——一条脏记录不该让整份列表读不出来。
    """
    text = str(raw).strip() if raw is not None else ""
    if not text:
        return ""
    if field == "email":
        return text.lower()
    if field == "phone":
        return normalize_phone(text) or text
    raise ValueError(f"不支持的地址字段：{field}")


def is_valid_address(field: str, raw: object) -> bool:
    """地址是否合法（空值视为「未绑定」，返回 ``True``）。"""
    try:
        normalize_address(field, raw)
    except AddressFormatError:
        return False
    return True


__all__ = [
    "ADDRESS_FIELDS",
    "AddressFormatError",
    "DOMAIN_LABEL_MAX",
    "DOMAIN_MAX_LENGTH",
    "EMAIL_LOCAL_MAX",
    "EMAIL_MAX_LENGTH",
    "address_key",
    "is_valid_address",
    "normalize_address",
    "normalize_email",
    "normalize_phone_address",
]
