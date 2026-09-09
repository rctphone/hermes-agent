"""An ambiguous Telegram edit must keep updating the existing stream message."""

from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest
from telegram.error import BadRequest, NetworkError

from gateway.config import PlatformConfig
from gateway.platforms.base import SendResult
from gateway.run_turn import GatewayTurnMixin
from gateway.stream_consumer import GatewayStreamConsumer, StreamConsumerConfig
from plugins.platforms.telegram.adapter import TelegramAdapter


@pytest.mark.asyncio
@pytest.mark.parametrize("phase", ["progress", "final"])
@pytest.mark.parametrize("accepted", [False, True])
async def test_transient_edit_reuses_message_and_confirms_final(phase, accepted):
    adapter = TelegramAdapter(PlatformConfig(enabled=True, token="123:fake", extra={}))
    visible = {}
    failed = False
    prefix, progress, final = "Reason", "Reason LinkedIn", "Reason LinkedIn did not open the post"

    async def send(**kwargs):
        mid = len(visible) + 1
        visible[mid] = kwargs["text"]
        return SimpleNamespace(message_id=mid)

    async def edit(**kwargs):
        nonlocal failed
        mid, text = kwargs["message_id"], kwargs["text"]
        target = progress if phase == "progress" else final
        if not failed and text == target:
            failed = True
            if accepted:
                visible[mid] = text
            raise NetworkError("httpx.ReadError: connection closed")
        if visible[mid] == text:
            raise BadRequest("Message is not modified")
        visible[mid] = text

    adapter._bot = SimpleNamespace(
        send_message=AsyncMock(side_effect=send),
        edit_message_text=AsyncMock(side_effect=edit),
        send_chat_action=AsyncMock(),
    )
    consumer = GatewayStreamConsumer(adapter, "123", config=StreamConsumerConfig(cursor=""))
    await consumer._send_or_edit(prefix)
    if phase == "progress":
        await consumer._send_or_edit(progress)
    consumer.finish(final)
    await consumer.run()

    assert failed
    assert visible == {1: final}
    assert adapter._bot.send_message.await_count == 1
    assert {c.kwargs["message_id"] for c in adapter._bot.edit_message_text.await_args_list} == {1}
    assert GatewayTurnMixin._run_agent_stream_confirmed_final_delivery(consumer, final)


@pytest.mark.asyncio
@pytest.mark.parametrize("failure,attempts", [
    (SendResult(success=False, error="httpx.ReadError", retryable=True), 3),
    (SendResult(success=False, error="message to edit not found"), 1),
    (SendResult(success=False, error="flood_control:60", retryable=True), 1),
    (SendResult(success=False, retryable=True, raw_response={"partial_overflow": True}), 1),
])
async def test_edit_retries_are_bounded_and_do_not_replay_partial_sends(failure, attempts):
    adapter = SimpleNamespace(edit_message=AsyncMock(return_value=failure))
    consumer = GatewayStreamConsumer(adapter, "123")
    assert await consumer._edit_message(message_id="1", content="complete answer") is failure
    assert adapter.edit_message.await_count == attempts
