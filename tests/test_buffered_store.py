import asyncio
from datetime import UTC, datetime
from unittest.mock import AsyncMock, MagicMock

import pytest

from knx_telegram_store import BufferedTelegramStore, StoredTelegram, TelegramQuery


@pytest.fixture
def inner_store():
    store = MagicMock()
    store.store_many = AsyncMock()
    store.close = AsyncMock()
    store.initialize = AsyncMock()
    store.query = AsyncMock()
    store.count = AsyncMock()
    store.evict_older_than = AsyncMock()
    store.evict_expired = AsyncMock()
    store.clear = AsyncMock()
    store.capabilities = MagicMock()
    store.retention_days = 10
    store.max_telegrams = 1000
    return store


@pytest.fixture
def buffered_store(inner_store):
    return BufferedTelegramStore(inner_store, flush_interval=0.1)


@pytest.fixture
def sample_telegram():
    return StoredTelegram(
        timestamp=datetime.now(UTC),
        source="1.1.1",
        destination="1/1/1",
        telegramtype="GroupValueWrite",
        direction="Incoming",
    )


@pytest.mark.asyncio
async def test_store_buffers_until_flush(buffered_store, inner_store, sample_telegram):
    buffered_store.store(sample_telegram)
    # Inner store should not have been called yet
    inner_store.store_many.assert_not_called()

    await buffered_store.flush()
    inner_store.store_many.assert_called_once_with([sample_telegram])


@pytest.mark.asyncio
async def test_periodic_flush(buffered_store, inner_store, sample_telegram):
    buffered_store.start()
    buffered_store.store(sample_telegram)

    # Wait for periodic flush (flush_interval is 0.1)
    await asyncio.sleep(0.2)

    inner_store.store_many.assert_called_once_with([sample_telegram])
    await buffered_store.stop()


@pytest.mark.asyncio
async def test_stop_flushes_remaining(buffered_store, inner_store, sample_telegram):
    buffered_store.store(sample_telegram)
    await buffered_store.stop()

    inner_store.store_many.assert_called_once_with([sample_telegram])
    inner_store.close.assert_called_once()


@pytest.mark.asyncio
async def test_flush_failure_reprepends(buffered_store, inner_store, sample_telegram):
    inner_store.store_many.side_effect = Exception("DB error")

    buffered_store.store(sample_telegram)
    await buffered_store.flush()

    # Buffer should still contain the telegram
    assert len(buffered_store._buffer) == 1
    assert buffered_store._buffer[0] == sample_telegram


@pytest.mark.asyncio
async def test_flush_failure_then_recovery(buffered_store, inner_store, sample_telegram):
    inner_store.store_many.side_effect = [Exception("DB error"), None]

    buffered_store.store(sample_telegram)
    await buffered_store.flush()
    assert len(buffered_store._buffer) == 1

    await buffered_store.flush()
    assert len(buffered_store._buffer) == 0
    assert inner_store.store_many.call_count == 2


@pytest.mark.asyncio
async def test_store_many_passes_through(buffered_store, inner_store, sample_telegram):
    telegrams = [sample_telegram]
    await buffered_store.store_many(telegrams)
    inner_store.store_many.assert_called_once_with(telegrams)


@pytest.mark.asyncio
async def test_read_operations_pass_through(buffered_store, inner_store):
    query = TelegramQuery()
    await buffered_store.query(query)
    inner_store.query.assert_called_once_with(query)

    await buffered_store.count()
    inner_store.count.assert_called_once()

    cutoff = datetime.now(UTC)
    await buffered_store.evict_older_than(cutoff, dry_run=True)
    inner_store.evict_older_than.assert_called_once_with(cutoff, dry_run=True)

    await buffered_store.evict_expired(dry_run=True)
    inner_store.evict_expired.assert_called_once_with(dry_run=True)


@pytest.mark.asyncio
async def test_flush_empty_buffer_is_noop(buffered_store, inner_store):
    await buffered_store.flush()
    inner_store.store_many.assert_not_called()


def test_properties_delegate(buffered_store, inner_store):
    assert buffered_store.capabilities == inner_store.capabilities
    assert buffered_store.retention_days == 10
    assert buffered_store.max_telegrams == 1000
