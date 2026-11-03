"""偏好模型、三层解析与存储后端的测试。"""
from __future__ import annotations

import json
import threading

import pytest

from app.models.channel import Channel
from app.preferences import (
    JsonlPreferenceStore,
    MemoryPreferenceStore,
    Preference,
    PreferenceSet,
    Scope,
    create_preference_store,
    normalize_target,
    preference_from_row,
    preference_to_row,
)


def _pref(subscriber_id: str = "creator_1", scope=Scope.GLOBAL, target=None, enabled=True):
    return Preference(
        subscriber_id=subscriber_id, scope=scope, target=target, enabled=enabled
    )


# ---------------------------------------------------------------- 模型与校验


def test_global_preference_rejects_target():
    with pytest.raises(ValueError):
        _pref(scope=Scope.GLOBAL, target="post_liked")


@pytest.mark.parametrize("scope", [Scope.WORKFLOW, Scope.CHANNEL])
def test_scoped_preference_requires_target(scope):
    with pytest.raises(ValueError):
        _pref(scope=scope, target="")


@pytest.mark.parametrize("bad", ["delay", "digest"])
def test_channel_preference_rejects_orchestration_steps(bad):
    with pytest.raises(ValueError):
        _pref(scope=Scope.CHANNEL, target=bad)


def test_channel_preference_rejects_unknown_channel():
    with pytest.raises(ValueError):
        _pref(scope=Scope.CHANNEL, target="carrier_pigeon")


@pytest.mark.parametrize("bad", ["", "   ", None])
def test_subscriber_id_is_required(bad):
    with pytest.raises(ValueError):
        Preference(subscriber_id=bad, scope=Scope.GLOBAL)


def test_subscriber_id_length_is_bounded():
    with pytest.raises(ValueError):
        Preference(subscriber_id="x" * 200, scope=Scope.GLOBAL)


def test_workflow_name_rejects_whitespace():
    with pytest.raises(ValueError):
        _pref(scope=Scope.WORKFLOW, target="post liked")


def test_workflow_name_is_trimmed():
    pref = _pref(scope=Scope.WORKFLOW, target="  post_liked  ")
    assert pref.target == "post_liked"


def test_channel_target_is_normalized_to_value():
    pref = _pref(scope=Scope.CHANNEL, target=Channel.EMAIL)
    assert pref.target == "email"
    assert pref.key == ("creator_1", "channel", "email")


def test_preference_defaults_to_enabled_with_timestamp():
    pref = _pref()
    assert pref.enabled is True
    assert pref.updated_at.tzinfo is not None


def test_normalize_target_returns_none_for_global():
    assert normalize_target(Scope.GLOBAL, "") is None
    assert normalize_target(Scope.GLOBAL, None) is None


def test_describe_covers_all_scopes():
    assert _pref(scope=Scope.GLOBAL, enabled=False).describe() == "creator_1 全局不接收"
    assert (
        _pref(scope=Scope.WORKFLOW, target="post_liked").describe()
        == "creator_1 的工作流 post_liked 接收"
    )
    assert (
        _pref(scope=Scope.CHANNEL, target="sms", enabled=False).describe()
        == "creator_1 的渠道 sms 不接收"
    )


# ---------------------------------------------------------------- 三层解析


def test_resolve_falls_back_to_accept_when_nothing_is_set():
    decision = PreferenceSet().resolve("creator_1", workflow="post_liked", channel="email")
    assert decision.allowed is True
    assert decision.matched is False
    assert decision.scope is None
    assert "默认接收" in decision.reason


def test_channel_preference_overrides_global():
    snapshot = PreferenceSet(
        [
            _pref(scope=Scope.GLOBAL, enabled=False),
            _pref(scope=Scope.CHANNEL, target="email", enabled=True),
        ]
    )
    decision = snapshot.resolve("creator_1", workflow="post_liked", channel="email")
    assert decision.allowed is True
    assert decision.scope is Scope.CHANNEL
    # 没有单独配置的渠道仍回到全局判断。
    blocked = snapshot.resolve("creator_1", workflow="post_liked", channel="sms")
    assert blocked.allowed is False
    assert blocked.scope is Scope.GLOBAL


def test_channel_preference_overrides_workflow():
    snapshot = PreferenceSet(
        [
            _pref(scope=Scope.WORKFLOW, target="post_liked", enabled=True),
            _pref(scope=Scope.CHANNEL, target="email", enabled=False),
        ]
    )
    blocked = snapshot.resolve("creator_1", workflow="post_liked", channel="email")
    assert blocked.allowed is False
    assert blocked.scope is Scope.CHANNEL
    assert "email" in blocked.reason

    allowed = snapshot.resolve("creator_1", workflow="post_liked", channel="in_app")
    assert allowed.allowed is True
    assert allowed.scope is Scope.WORKFLOW


def test_workflow_preference_overrides_global():
    snapshot = PreferenceSet(
        [
            _pref(scope=Scope.GLOBAL, enabled=True),
            _pref(scope=Scope.WORKFLOW, target="post_commented", enabled=False),
        ]
    )
    assert snapshot.resolve("creator_1", workflow="post_commented").allowed is False
    # 同一个人的另一类互动不受影响。
    assert snapshot.resolve("creator_1", workflow="post_liked").allowed is True


def test_resolve_isolates_subscribers():
    snapshot = PreferenceSet([_pref("creator_1", scope=Scope.GLOBAL, enabled=False)])
    assert snapshot.resolve("creator_1", channel="email").allowed is False
    assert snapshot.resolve("creator_2", channel="email").allowed is True


def test_resolve_without_channel_or_workflow_only_reads_global():
    snapshot = PreferenceSet(
        [
            _pref(scope=Scope.CHANNEL, target="email", enabled=False),
            _pref(scope=Scope.GLOBAL, enabled=True),
        ]
    )
    decision = snapshot.resolve("creator_1")
    assert decision.allowed is True
    assert decision.scope is Scope.GLOBAL


def test_resolve_rejects_orchestration_channel():
    with pytest.raises(ValueError):
        PreferenceSet().resolve("creator_1", channel="digest")


def test_decision_serializes_for_http_response():
    payload = PreferenceSet().resolve("creator_1", channel="email").as_dict()
    assert payload == {
        "allowed": True,
        "scope": None,
        "target": None,
        "matched": False,
        "reason": "未设置偏好，按默认接收",
    }


def test_snapshot_keeps_last_record_for_same_key():
    first = _pref(scope=Scope.GLOBAL, enabled=True)
    second = _pref(scope=Scope.GLOBAL, enabled=False)
    snapshot = PreferenceSet([first, second])
    assert len(snapshot) == 1
    assert snapshot.resolve("creator_1").allowed is False


def test_snapshot_records_follow_specificity_order():
    snapshot = PreferenceSet(
        [
            _pref(scope=Scope.GLOBAL),
            _pref(scope=Scope.WORKFLOW, target="post_liked"),
            _pref(scope=Scope.CHANNEL, target="email"),
        ]
    )
    assert [item.scope for item in snapshot.records()] == [
        Scope.CHANNEL,
        Scope.WORKFLOW,
        Scope.GLOBAL,
    ]


def test_snapshot_get_accepts_unnormalized_target():
    snapshot = PreferenceSet([_pref(scope=Scope.CHANNEL, target="email")])
    assert snapshot.get("creator_1", Scope.CHANNEL, "email") is not None
    assert snapshot.get("creator_1", Scope.CHANNEL, Channel.EMAIL) is not None
    assert snapshot.get("creator_1", Scope.CHANNEL, "sms") is None


# ---------------------------------------------------------------- 行格式


def test_row_roundtrip_keeps_fields():
    pref = _pref(scope=Scope.CHANNEL, target="push", enabled=False)
    row = preference_to_row(pref)
    assert row["scope"] == "channel"
    assert row["enabled"] is False
    assert row["updated_at"].endswith("+00:00")
    restored = preference_from_row(json.loads(json.dumps(row)))
    assert restored == pref


@pytest.mark.parametrize(
    "row",
    [
        "not-a-dict",
        {"subscriber_id": "creator_1", "scope": "carrier_pigeon"},
        {"subscriber_id": "", "scope": "global"},
        {"subscriber_id": "creator_1", "scope": "channel", "target": "delay"},
        {"subscriber_id": "creator_1", "scope": "global", "target": "post_liked"},
    ],
)
def test_preference_from_row_skips_bad_rows(row):
    assert preference_from_row(row) is None


# ---------------------------------------------------------------- 内存存储


def test_memory_store_upsert_replaces_same_key():
    store = MemoryPreferenceStore()
    store.upsert(_pref(scope=Scope.WORKFLOW, target="post_liked", enabled=True))
    store.upsert(_pref(scope=Scope.WORKFLOW, target="post_liked", enabled=False))
    assert len(store) == 1
    found = store.get("creator_1", Scope.WORKFLOW, "post_liked")
    assert found is not None and found.enabled is False


def test_memory_store_filters_and_sorts():
    store = MemoryPreferenceStore()
    store.upsert(_pref("creator_2", scope=Scope.GLOBAL))
    store.upsert(_pref("creator_1", scope=Scope.GLOBAL))
    store.upsert(_pref("creator_1", scope=Scope.CHANNEL, target="email"))

    assert [p.subscriber_id for p in store.list()] == [
        "creator_1",
        "creator_1",
        "creator_2",
    ]
    assert [p.scope for p in store.list(subscriber_id="creator_1")] == [
        Scope.CHANNEL,
        Scope.GLOBAL,
    ]
    assert [p.scope for p in store.list(scope=Scope.GLOBAL)] == [
        Scope.GLOBAL,
        Scope.GLOBAL,
    ]


def test_memory_store_delete():
    store = MemoryPreferenceStore()
    store.upsert(_pref(scope=Scope.CHANNEL, target="sms"))
    assert store.delete("creator_1", Scope.CHANNEL, "sms") is True
    assert store.delete("creator_1", Scope.CHANNEL, "sms") is False
    assert len(store) == 0


def test_memory_store_snapshot_is_isolated():
    store = MemoryPreferenceStore()
    store.upsert(_pref(scope=Scope.GLOBAL, enabled=True))
    snapshot = store.snapshot()
    store.upsert(_pref(scope=Scope.GLOBAL, enabled=False))
    assert snapshot.resolve("creator_1").allowed is True
    assert store.snapshot().resolve("creator_1").allowed is False


def test_memory_store_survives_concurrent_upserts():
    store = MemoryPreferenceStore()

    def writer(index: int) -> None:
        for round_ in range(20):
            store.upsert(_pref(f"creator_{index}", scope=Scope.WORKFLOW, target=f"wf_{round_}"))

    threads = [threading.Thread(target=writer, args=(index,)) for index in range(8)]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join()

    assert len(store) == 8 * 20
    assert len(store.list(subscriber_id="creator_3")) == 20


# ---------------------------------------------------------------- 文件存储


def test_jsonl_store_persists_and_reloads(tmp_path):
    path = tmp_path / "preferences.jsonl"
    store = JsonlPreferenceStore(path)
    store.upsert(_pref(scope=Scope.GLOBAL, enabled=False))
    store.upsert(_pref(scope=Scope.CHANNEL, target="email", enabled=True))

    reloaded = JsonlPreferenceStore(path)
    assert len(reloaded) == 2
    assert reloaded.snapshot().resolve("creator_1", channel="email").allowed is True
    assert reloaded.snapshot().resolve("creator_1", channel="sms").allowed is False


def test_jsonl_store_rewrites_instead_of_appending(tmp_path):
    path = tmp_path / "preferences.jsonl"
    store = JsonlPreferenceStore(path)
    for enabled in (True, False, True, False):
        store.upsert(_pref(scope=Scope.WORKFLOW, target="post_liked", enabled=enabled))

    lines = [line for line in path.read_text(encoding="utf-8").splitlines() if line.strip()]
    assert len(lines) == 1
    assert json.loads(lines[0])["enabled"] is False


def test_jsonl_store_sees_writes_from_other_instance(tmp_path):
    path = tmp_path / "preferences.jsonl"
    writer = JsonlPreferenceStore(path)
    reader = JsonlPreferenceStore(path)
    assert len(reader) == 0

    writer.upsert(_pref(scope=Scope.GLOBAL, enabled=False))
    # 读之前按文件大小与修改时刻判断，被别的进程写过就重扫。
    assert len(reader) == 1
    assert reader.snapshot().resolve("creator_1").allowed is False


def test_jsonl_store_delete_and_missing_key(tmp_path):
    path = tmp_path / "preferences.jsonl"
    store = JsonlPreferenceStore(path)
    store.upsert(_pref(scope=Scope.CHANNEL, target="push"))
    assert store.delete("creator_1", Scope.CHANNEL, "push") is True
    assert store.delete("creator_1", Scope.CHANNEL, "push") is False
    assert path.read_text(encoding="utf-8").strip() == ""


def test_jsonl_store_skips_corrupt_lines(tmp_path):
    path = tmp_path / "preferences.jsonl"
    path.write_text(
        "\n".join(
            [
                "{not json",
                json.dumps(
                    {
                        "subscriber_id": "creator_1",
                        "scope": "channel",
                        "target": "delay",
                        "enabled": True,
                    }
                ),
                json.dumps(
                    {
                        "subscriber_id": "creator_2",
                        "scope": "global",
                        "target": None,
                        "enabled": False,
                        "updated_at": "2026-11-03T12:00:00+00:00",
                    }
                ),
            ]
        )
        + "\n",
        encoding="utf-8",
    )
    store = JsonlPreferenceStore(path)
    assert len(store) == 1
    assert store.snapshot().resolve("creator_2").allowed is False


def test_create_preference_store_switches_backend(tmp_path, monkeypatch):
    assert isinstance(create_preference_store("memory"), MemoryPreferenceStore)

    path = tmp_path / "custom.jsonl"
    assert isinstance(create_preference_store("jsonl", path), JsonlPreferenceStore)

    monkeypatch.setenv("XW_PREFERENCES_FILE", str(path))
    from_env = create_preference_store("jsonl")
    assert isinstance(from_env, JsonlPreferenceStore)
    assert from_env.path == path
