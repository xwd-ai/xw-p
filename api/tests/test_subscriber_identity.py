"""订阅者地址的格式校验与唯一性测试。

覆盖三层：规整函数本身（邮箱的字符与结构规则、手机号转 E.164）、存储层的
查重索引（内存与 JSONL 两种后端、并发写入）、以及接口层的状态码与错误文案
（格式问题 422、被占用 409）。
"""
from __future__ import annotations

import threading

import pytest
from fastapi.testclient import TestClient

from app import config
from app.main import app
from app.models.channel import Channel
from app.models.entities import Environment, Subscriber
from app.subscriber_identity import (
    AddressFormatError,
    address_key,
    is_valid_address,
    normalize_address,
    normalize_email,
    normalize_phone_address,
)
from app.subscriber_store import (
    JsonlSubscriberStore,
    MemorySubscriberStore,
    SubscriberAddressConflictError,
    SubscriberConflictError,
    SubscriberNotFoundError,
)


def _subscriber(subscriber_id: str = "creator_1", **kwargs) -> Subscriber:
    return Subscriber(id=subscriber_id, **kwargs)


@pytest.fixture()
def store():
    return MemorySubscriberStore()


@pytest.fixture()
def client(monkeypatch, store):
    monkeypatch.setattr("app.subscriber_routes.get_subscriber_store", lambda: store)
    with TestClient(app) as test_client:
        yield test_client


# ---------------------------------------------------------------- 邮箱规整


def test_email_is_lowercased_and_trimmed():
    assert normalize_email("  Creator@Example.COM  ") == "creator@example.com"
    # 用户名里的点、加号与下划线都是合法字符，不该被动过。
    assert normalize_email("first.last+tag@sub.example.co") == "first.last+tag@sub.example.co"
    assert normalize_email("a_b-c@example.io") == "a_b-c@example.io"


@pytest.mark.parametrize(
    ("raw", "hint"),
    [
        ("", "不能为空"),
        ("   ", "不能为空"),
        ("creator", "恰好有一个 @"),
        ("a@@example.com", "恰好有一个 @"),
        ("@example.com", "缺少 @ 前面的用户名"),
        ("creator@", "缺少 @ 后面的域名"),
        ("creator@example", "缺少顶级域名"),
        ("creator@example.c", "顶级域名不合法"),
        ("creator@-example.com", "域名段落不合法"),
        ("creator@exa_mple.com", "域名段落不合法"),
        ("creator@example..com", "不能出现连续的点"),
        (".creator@example.com", "不能以点开头或结尾"),
        ("creator.@example.com", "不能以点开头或结尾"),
        ("crea..tor@example.com", "不能出现连续的点"),
        ("cre ator@example.com", "空白字符"),
        ("creator@例え.jp", "punycode"),
        ("creator,name@example.com", "不允许的字符"),
    ],
)
def test_email_rejects_invalid_forms(raw, hint):
    with pytest.raises(AddressFormatError) as excinfo:
        normalize_email(raw)
    assert excinfo.value.field == "email"
    assert hint in str(excinfo.value)


def test_email_enforces_length_limits():
    long_local = "a" * 65
    with pytest.raises(AddressFormatError, match="用户名不能超过"):
        normalize_email(f"{long_local}@example.com")

    long_label = "a" * 64
    with pytest.raises(AddressFormatError, match="域名段落不能超过"):
        normalize_email(f"creator@{long_label}.com")

    with pytest.raises(AddressFormatError, match="长度不能超过"):
        normalize_email("a" * 250 + "@example.com")


# ---------------------------------------------------------------- 手机号规整


def test_phone_normalizes_separators():
    assert normalize_phone_address("+8613800000000") == "+8613800000000"
    assert normalize_phone_address(" +86 138-0000-0000 ") == "+8613800000000"
    assert normalize_phone_address("(+86) 138 0000 0000") == "+8613800000000"


def test_phone_fills_default_country_code():
    assert normalize_phone_address("13800000000", "86") == "+8613800000000"
    assert normalize_phone_address("13800000000", "+86") == "+8613800000000"


def test_phone_without_country_code_is_rejected():
    with pytest.raises(AddressFormatError) as excinfo:
        normalize_phone_address("13800000000", "")
    assert "国家码" in str(excinfo.value) or "区号" in str(excinfo.value)


def test_phone_uses_configured_country_code(monkeypatch):
    monkeypatch.setattr(config, "SUBSCRIBER_COUNTRY_CODE", "86")
    assert normalize_phone_address("13800000000") == "+8613800000000"
    # 自带区号的号码不受默认值影响。
    assert normalize_phone_address("+14155550123") == "+14155550123"


@pytest.mark.parametrize(
    "raw",
    ["", "   ", "abc", "12", "+8613800000000000000", "138+00000000", "138 0000 0000 x"],
)
def test_phone_rejects_invalid_forms(raw):
    with pytest.raises(AddressFormatError):
        normalize_phone_address(raw, "86")


# ---------------------------------------------------------------- 比较键


def test_address_key_ignores_case_and_separators():
    assert address_key("email", "Creator@Example.COM") == "creator@example.com"
    assert address_key("phone", "+86 138 0000 0000") == "+8613800000000"
    # 规整不了的脏数据退回原文，读路径不因为一条历史记录炸掉。
    assert address_key("phone", "not-a-number") == "not-a-number"
    assert address_key("email", "") == ""


def test_normalize_address_handles_blank_as_unbound():
    assert normalize_address("email", "") is None
    assert normalize_address("email", None) is None
    assert normalize_address("phone", "  ") is None
    with pytest.raises(ValueError):
        normalize_address("device_token", "abc")


def test_is_valid_address_treats_blank_as_valid():
    assert is_valid_address("email", None)
    assert is_valid_address("email", "creator@example.com")
    assert not is_valid_address("email", "creator@example")


# ---------------------------------------------------------------- 存储层查重


def test_memory_store_rejects_duplicate_email_ignoring_case():
    store = MemorySubscriberStore()
    store.create(_subscriber("creator_1", email="creator@example.com"))

    with pytest.raises(SubscriberAddressConflictError) as excinfo:
        store.create(_subscriber("creator_2", email="Creator@Example.com"))
    assert excinfo.value.field == "email"
    assert excinfo.value.subscriber_id == "creator_1"
    assert "creator_1" in str(excinfo.value)
    # 被拒的写入不留痕。
    assert store.get("creator_2") is None


def test_memory_store_rejects_duplicate_phone_ignoring_separators():
    store = MemorySubscriberStore()
    store.create(_subscriber("creator_1", phone="+8613800000000"))

    with pytest.raises(SubscriberAddressConflictError) as excinfo:
        store.create(_subscriber("creator_2", phone="+86 138 0000 0000"))
    assert excinfo.value.field == "phone"


def test_memory_store_allows_reusing_own_address_on_update():
    store = MemorySubscriberStore()
    created = store.create(_subscriber("creator_1", email="creator@example.com"))

    saved = store.update(created.model_copy(update={"channels": [Channel.EMAIL]}))
    assert saved.email == "creator@example.com"

    # 同一记录里把邮箱换个写法（大小写不同）也认成自己，不该自相冲突。
    store.update(created.model_copy(update={"email": "Creator@Example.com"}))
    assert store.get("creator_1").email == "Creator@Example.com"


def test_memory_store_update_conflicting_address_keeps_original():
    store = MemorySubscriberStore()
    first = store.create(_subscriber("creator_1", email="first@example.com"))
    store.create(_subscriber("creator_2", email="second@example.com"))

    with pytest.raises(SubscriberAddressConflictError):
        store.update(first.model_copy(update={"email": "second@example.com"}))
    assert store.get("creator_1").email == "first@example.com"


def test_deleting_frees_the_address():
    store = MemorySubscriberStore()
    store.create(_subscriber("creator_1", email="creator@example.com"))
    assert store.delete("creator_1") is True
    store.create(_subscriber("creator_2", email="creator@example.com"))
    assert store.get("creator_2").email == "creator@example.com"


def test_unbinding_frees_the_address():
    store = MemorySubscriberStore()
    created = store.create(_subscriber("creator_1", email="creator@example.com"))
    store.update(created.model_copy(update={"email": None}))
    store.create(_subscriber("creator_2", email="creator@example.com"))
    assert len(store) == 2


def test_uniqueness_is_scoped_per_environment():
    store = MemorySubscriberStore()
    store.create(
        _subscriber(
            "creator_dev",
            email="creator@example.com",
            environment=Environment.DEVELOPMENT,
        )
    )
    # 同一个邮箱在生产环境有独立档案是正常的：两套环境是两套数据。
    store.create(
        _subscriber(
            "creator_prod",
            email="creator@example.com",
            environment=Environment.PRODUCTION,
        )
    )
    assert len(store) == 2


def test_memory_store_still_rejects_duplicate_id_first():
    store = MemorySubscriberStore()
    store.create(_subscriber("creator_1", email="a@example.com"))
    with pytest.raises(SubscriberConflictError):
        store.create(_subscriber("creator_1", email="b@example.com"))


def test_memory_store_concurrent_create_keeps_one():
    """并发抢同一个邮箱：只有一条能落地，另一条如实报冲突。"""
    store = MemorySubscriberStore()
    barrier = threading.Barrier(8)
    results: list[str] = []

    def attempt(index: int) -> None:
        barrier.wait()
        try:
            store.create(_subscriber(f"creator_{index}", email="shared@example.com"))
            results.append("ok")
        except SubscriberAddressConflictError:
            results.append("conflict")

    threads = [threading.Thread(target=attempt, args=(i,)) for i in range(8)]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join()

    assert results.count("ok") == 1
    assert results.count("conflict") == 7
    assert len(store) == 1


def test_jsonl_store_rebuilds_index_after_restart(tmp_path):
    path = tmp_path / "subscribers.jsonl"
    first = JsonlSubscriberStore(path)
    first.create(_subscriber("creator_1", email="creator@example.com"))

    # 新实例只靠文件恢复索引，唯一性判定不能因为重启而失效。
    second = JsonlSubscriberStore(path)
    with pytest.raises(SubscriberAddressConflictError):
        second.create(_subscriber("creator_2", email="CREATOR@example.com"))

    assert second.delete("creator_1") is True
    second.create(_subscriber("creator_2", email="creator@example.com"))
    assert second.get("creator_2") is not None


def test_jsonl_store_detects_conflict_from_other_instance(tmp_path):
    path = tmp_path / "subscribers.jsonl"
    a = JsonlSubscriberStore(path)
    b = JsonlSubscriberStore(path)

    a.create(_subscriber("creator_1", email="creator@example.com"))
    with pytest.raises(SubscriberAddressConflictError):
        # b 的索引是在 a 写入之前建的：写入前要按文件变化重扫。
        b.create(_subscriber("creator_2", email="creator@example.com"))
    assert b.get("creator_2") is None


def test_jsonl_store_update_conflict_keeps_file_intact(tmp_path):
    path = tmp_path / "subscribers.jsonl"
    store = JsonlSubscriberStore(path)
    first = store.create(_subscriber("creator_1", email="first@example.com"))
    store.create(_subscriber("creator_2", email="second@example.com"))

    with pytest.raises(SubscriberAddressConflictError):
        store.update(first.model_copy(update={"email": "second@example.com"}))

    rows = [line for line in path.read_text(encoding="utf-8").splitlines() if line.strip()]
    assert len(rows) == 2
    assert JsonlSubscriberStore(path).get("creator_1").email == "first@example.com"


def test_jsonl_store_ignores_corrupt_address_when_indexing(tmp_path):
    path = tmp_path / "subscribers.jsonl"
    path.write_text(
        '{"id":"creator_1","email":"creator@example.com","channels":["email"]}\n'
        '{"id":"creator_2","phone":"not-a-number","channels":["sms"]}\n',
        encoding="utf-8",
    )
    store = JsonlSubscriberStore(path)
    # 脏手机号不参与查重，但记录本身照常读得出来。
    assert store.get("creator_2").phone == "not-a-number"
    with pytest.raises(SubscriberAddressConflictError):
        store.create(_subscriber("creator_3", email="Creator@Example.com"))


# ---------------------------------------------------------------- 接口层


def test_create_subscriber_normalizes_addresses(client):
    response = client.post(
        "/v1/subscribers",
        json={
            "id": "creator_1",
            "email": "  Creator@Example.COM ",
            "phone": "+86 138 0000 0000",
            "channels": ["email", "sms"],
        },
    )
    assert response.status_code == 201
    body = response.json()
    assert body["email"] == "creator@example.com"
    assert body["phone"] == "+8613800000000"
    assert body["bindings"] == {
        "email": "creator@example.com",
        "sms": "+8613800000000",
    }


@pytest.mark.parametrize(
    "payload",
    [
        {"email": "creator@example"},
        {"email": "creator@@example.com"},
        {"phone": "1380000000"},
    ],
)
def test_create_subscriber_rejects_invalid_address(client, monkeypatch, payload):
    # 没配默认国家码时本地号码不被接受：不猜区号是刻意的行为。
    monkeypatch.setattr(config, "SUBSCRIBER_COUNTRY_CODE", "")
    response = client.post("/v1/subscribers", json=payload)
    assert response.status_code == 422
    detail = response.text
    assert "邮箱" in detail or "手机号" in detail


def test_create_subscriber_conflicting_email_returns_409(client):
    assert (
        client.post(
            "/v1/subscribers", json={"id": "creator_1", "email": "creator@example.com"}
        ).status_code
        == 201
    )
    conflict = client.post(
        "/v1/subscribers", json={"id": "creator_2", "email": "Creator@Example.com"}
    )
    assert conflict.status_code == 409
    assert "creator_1" in conflict.json()["detail"]


def test_create_subscriber_conflicting_phone_returns_409(client):
    assert (
        client.post(
            "/v1/subscribers", json={"id": "creator_1", "phone": "+8613800000000"}
        ).status_code
        == 201
    )
    conflict = client.post(
        "/v1/subscribers", json={"id": "creator_2", "phone": "+86 138 0000 0000"}
    )
    assert conflict.status_code == 409
    assert "手机号" in conflict.json()["detail"]


def test_update_subscriber_conflicting_email_returns_409(client):
    client.post("/v1/subscribers", json={"id": "creator_1", "email": "first@example.com"})
    client.post("/v1/subscribers", json={"id": "creator_2", "email": "second@example.com"})

    response = client.put(
        "/v1/subscribers/creator_2", json={"email": "first@example.com"}
    )
    assert response.status_code == 409
    assert client.get("/v1/subscribers/creator_2").json()["email"] == "second@example.com"


def test_update_subscriber_rejects_invalid_address(client):
    client.post("/v1/subscribers", json={"id": "creator_1", "channels": ["email"]})
    response = client.put("/v1/subscribers/creator_1", json={"email": "creator@example"})
    assert response.status_code == 422


def test_update_subscriber_keeps_own_address(client):
    client.post(
        "/v1/subscribers",
        json={"id": "creator_1", "email": "creator@example.com", "channels": ["email"]},
    )
    response = client.put(
        "/v1/subscribers/creator_1", json={"email": "Creator@Example.com"}
    )
    assert response.status_code == 200
    assert response.json()["email"] == "creator@example.com"


def test_unbound_address_can_be_claimed(client):
    client.post("/v1/subscribers", json={"id": "creator_1", "email": "creator@example.com"})
    client.put("/v1/subscribers/creator_1", json={"email": ""})
    claimed = client.post(
        "/v1/subscribers", json={"id": "creator_2", "email": "creator@example.com"}
    )
    assert claimed.status_code == 201


def test_update_missing_subscriber_still_404(client):
    # 格式合法才会走到「记录不存在」这一步，两条判定不要互相遮蔽。
    response = client.put("/v1/subscribers/ghost", json={"email": "ghost@example.com"})
    assert response.status_code == 404


def test_store_update_missing_subscriber_raises(tmp_path):
    for store in (MemorySubscriberStore(), JsonlSubscriberStore(tmp_path / "s.jsonl")):
        with pytest.raises(SubscriberNotFoundError):
            store.update(_subscriber("ghost"))
