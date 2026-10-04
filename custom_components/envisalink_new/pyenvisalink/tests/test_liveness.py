"""Tests for the command-timeout liveness rule in EnvisalinkClient.

A command whose acknowledgement is lost should not tear the session down while the EVL
is still sending data; a silent EVL must still be disconnected.
"""

import asyncio
import time
import unittest
from unittest.mock import AsyncMock, MagicMock

from pyenvisalink.envisalink_base_client import EnvisalinkClient


def _make_client(keepalive_interval=60, command_timeout=5):
    client = EnvisalinkClient.__new__(EnvisalinkClient)
    panel = MagicMock()
    panel.keepalive_interval = keepalive_interval
    panel.command_timeout = command_timeout
    client._alarmPanel = panel
    client._shutdown = False
    client._loggedin = True
    client._commandEvent = asyncio.Event()
    client._commandQueue = []
    client._cachedCode = None
    client._last_rx_time = 0
    client.disconnect = AsyncMock()
    client.send_command = AsyncMock()
    return client


async def _run_one_pass(client):
    """Run the command processor until it has handled the expired command, then stop it."""
    task = asyncio.ensure_future(client.process_command_queue())
    await asyncio.sleep(0.05)
    client._shutdown = True
    client._commandEvent.set()
    await asyncio.wait_for(task, 2)


def _expired_sent_op(client):
    op = EnvisalinkClient.Operation("00", "", None, "")
    op.state = EnvisalinkClient.Operation.State.SENT
    op.expiryTime = time.time() - 1
    client._commandQueue.append(op)
    return op


class TestLiveness(unittest.TestCase):
    def test_window_is_disabled_without_keepalive(self):
        client = _make_client(keepalive_interval=0)
        self.assertIsNone(client._liveness_window())
        client._last_rx_time = time.time()
        self.assertFalse(client._rx_within_liveness_window(time.time()))

    def test_window_follows_keepalive_interval(self):
        client = _make_client(keepalive_interval=60)
        self.assertEqual(client._liveness_window(), 150)

    def test_expiry_with_recent_rx_fails_command_without_disconnect(self):
        client = _make_client()
        client._last_rx_time = time.time()
        op = _expired_sent_op(client)
        with self.assertLogs("pyenvisalink.envisalink_base_client", level="WARNING"):
            asyncio.run(_run_one_pass(client))
        self.assertEqual(op.state, EnvisalinkClient.Operation.State.FAILED)
        client.disconnect.assert_not_awaited()

    def test_expiry_with_stale_rx_disconnects(self):
        client = _make_client()
        client._last_rx_time = time.time() - 1000
        op = _expired_sent_op(client)
        asyncio.run(_run_one_pass(client))
        self.assertEqual(op.state, EnvisalinkClient.Operation.State.FAILED)
        client.disconnect.assert_awaited()

    def test_expiry_without_keepalive_keeps_previous_behaviour(self):
        client = _make_client(keepalive_interval=0)
        client._last_rx_time = time.time()
        _expired_sent_op(client)
        asyncio.run(_run_one_pass(client))
        client.disconnect.assert_awaited()


if __name__ == "__main__":
    unittest.main()
