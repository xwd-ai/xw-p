"""触发接口：请求形态、回执解析、批量语义。"""
from __future__ import annotations

import pytest

from xw_notify import Event, NotFoundError, TriggerAck, TriggerBatch
from xw_notify.trigger import MAX_BATCH_EVENTS

from fakes import json_response, make_client

ACK = {
    "acknowledged": True,
    "triggerId": "trg-1",
    "transactionId": "txn-1",
    "acknowledgmentId": "trg-1",
}


def test_send_posts_full_event():
    client, transport = make_client(lambda request: json_response(200, ACK))

    ack = client.trigger.send(
        "post_commented",
        "creator-1001",
        payload={"post_id": "P-2026-1001", "actor": "阿澈", "comment": "求教程"},
        channels=["in_app", "email"],
        email="creator@example.com",
        phone="+8613800000000",
        device_token="fcm-9f2c1d",
        overrides={"step:email": {"subject": "新评论"}},
        idempotency_key="  comment-C-8842  ",
    )

    assert transport.last.method == "POST"
    assert transport.last.url == "http://api.test/v1/events/trigger"
    assert transport.last.headers["Idempotency-Key"] == "comment-C-8842"
    assert transport.last_json() == {
        "name": "post_commented",
        "to": {
            "subscriberId": "creator-1001",
            "email": "creator@example.com",
            "phone": "+8613800000000",
            "deviceToken": "fcm-9f2c1d",
            "channels": ["in_app", "email"],
        },
        "payload": {"post_id": "P-2026-1001", "actor": "阿澈", "comment": "求教程"},
        "overrides": {"step:email": {"subject": "新评论"}},
    }
    assert ack == TriggerAck(
        trigger_id="trg-1", transaction_id="txn-1", acknowledgment_id="trg-1"
    )


def test_minimal_event_omits_empty_sections():
    client, transport = make_client(lambda request: json_response(200, ACK))
    client.trigger.send("post_liked", "creator-1001", payload={"post_id": "P-1"})

    body = transport.last_json()
    assert body == {"name": "post_liked", "to": {"subscriberId": "creator-1001"}, "payload": {"post_id": "P-1"}}
    assert "Idempotency-Key" not in transport.last.headers


def test_blank_idempotency_key_is_ignored():
    client, transport = make_client(lambda request: json_response(200, ACK))
    client.trigger.send("post_liked", "c1", idempotency_key="   ")
    assert "Idempotency-Key" not in transport.last.headers


def test_payload_keys_are_not_rewritten():
    client, transport = make_client(lambda request: json_response(200, ACK))
    payload = {"post_id": "P-1", "postId": "P-1", "评论": "写得好", "nested": {"a_b": 1, "aB": 2}}
    client.trigger.send("post_liked", "c1", payload=payload)

    assert transport.last_json()["payload"] == payload


def test_channels_none_means_delegate_to_workflow():
    client, transport = make_client(lambda request: json_response(200, ACK))
    client.trigger.send("post_liked", "c1", channels=None)
    assert "channels" not in transport.last_json()["to"]


def test_empty_channel_list_is_sent_as_declared_zero():
    client, transport = make_client(lambda request: json_response(200, ACK))
    client.trigger.send("post_liked", "c1", channels=[])
    assert transport.last_json()["to"]["channels"] == []


def test_blank_recipient_fields_are_dropped():
    client, transport = make_client(lambda request: json_response(200, ACK))
    client.trigger.send("post_liked", "c1", email="   ", phone="")
    target = transport.last_json()["to"]
    assert "email" not in target
    assert "phone" not in target


@pytest.mark.parametrize("name", ["", "   "])
def test_blank_name_is_rejected_locally(name):
    client, transport = make_client()
    with pytest.raises(ValueError, match="name 不能为空"):
        client.trigger.send(name, "c1")
    assert transport.requests == []


def test_blank_subscriber_is_rejected_locally():
    client, transport = make_client()
    with pytest.raises(ValueError, match="subscriber_id 不能为空"):
        client.trigger.send("post_liked", "  ")
    assert transport.requests == []


def test_missing_workflow_surfaces_as_not_found():
    client, _ = make_client(
        lambda request: json_response(404, {"detail": "工作流不存在或未启用：post_liked（环境 production）"})
    )
    with pytest.raises(NotFoundError) as excinfo:
        client.trigger.send("post_liked", "c1")
    assert "未启用" in str(excinfo.value)


def test_event_can_be_built_and_reused():
    client, transport = make_client(lambda request: json_response(200, ACK))
    event = client.trigger.build("post_liked", "c1", payload={"post_id": "P-1"})
    assert isinstance(event, Event)
    client.trigger.send_event(event)
    client.trigger.send_event(event)
    assert len(transport.requests) == 2

    first = event.to_wire()
    first["payload"]["post_id"] = "改坏了"
    assert event.to_wire()["payload"] == {"post_id": "P-1"}


def test_event_wire_round_trip():
    event = Event(
        name="post_mentioned",
        subscriber_id="creator-1001",
        payload={"post_id": "P-1"},
        channels=("in_app", "push"),
        device_token="fcm-1",
    )
    restored = Event.from_wire(event.to_wire())
    assert restored == event


BATCH = {
    "acknowledged": True,
    "accepted": 2,
    "rejected": 1,
    "results": [
        {
            "index": 0,
            "name": "post_liked",
            "subscriberId": "c1",
            "status": "accepted",
            "triggerId": "trg-1",
            "transactionId": "txn-1",
            "acknowledgmentId": "trg-1",
        },
        {
            "index": 1,
            "name": "post_liked",
            "subscriberId": "c2",
            "status": "accepted",
            "triggerId": "trg-2",
            "transactionId": "txn-2",
            "acknowledgmentId": "trg-2",
        },
        {
            "index": 2,
            "name": "post_liked",
            "subscriberId": "c3",
            "status": "rejected",
            "error": "工作流不存在或未启用：post_liked",
        },
    ],
}


def test_batch_sends_all_events_in_one_request():
    client, transport = make_client(lambda request: json_response(200, BATCH))
    batch = client.trigger.send_batch(
        [
            client.trigger.build("post_liked", "c1", payload={"post_id": "P-1"}),
            client.trigger.build("post_liked", "c2", payload={"post_id": "P-1"}),
            client.trigger.build("post_liked", "c3", payload={"post_id": "P-1"}),
        ],
        idempotency_key="likes-P-1-batch-1",
    )

    assert transport.last.url == "http://api.test/v1/events/trigger/batch"
    assert transport.last.headers["Idempotency-Key"] == "likes-P-1-batch-1"
    assert [item["to"]["subscriberId"] for item in transport.last_json()["events"]] == ["c1", "c2", "c3"]

    assert isinstance(batch, TriggerBatch)
    assert (batch.accepted, batch.rejected, len(batch)) == (2, 1, 3)
    assert [item.subscriber_id for item in batch.accepted_items] == ["c1", "c2"]
    assert batch.rejected_items[0].error.startswith("工作流不存在")
    assert batch.rejected_items[0].accepted is False


def test_batch_indexes_align_with_request_order():
    client, _ = make_client(lambda request: json_response(200, BATCH))
    batch = client.trigger.send_batch(
        [client.trigger.build("post_liked", f"c{i}") for i in range(3)]
    )
    assert [item.index for item in batch] == [0, 1, 2]
    assert [item.subscriber_id for item in batch] == ["c1", "c2", "c3"]


def test_empty_batch_is_rejected_locally():
    client, transport = make_client()
    with pytest.raises(ValueError, match="至少需要一条事件"):
        client.trigger.send_batch([])
    assert transport.requests == []


def test_batch_over_limit_is_rejected_locally():
    client, transport = make_client()
    events = [client.trigger.build("post_liked", f"c{i}") for i in range(MAX_BATCH_EVENTS + 1)]
    with pytest.raises(ValueError, match=str(MAX_BATCH_EVENTS)):
        client.trigger.send_batch(events)
    assert transport.requests == []


def test_batch_rejects_foreign_objects():
    client, transport = make_client()
    with pytest.raises(TypeError, match="Event"):
        client.trigger.send_batch([{"name": "post_liked"}])  # type: ignore[list-item]
    assert transport.requests == []


def test_batch_accepts_generator():
    client, _ = make_client(lambda request: json_response(200, BATCH))
    events = (client.trigger.build("post_liked", f"c{i}") for i in range(3))
    batch = client.trigger.send_batch(events)
    assert len(batch) == 3
