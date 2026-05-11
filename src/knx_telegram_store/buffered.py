from __future__ import annotations

import asyncio
import logging
from collections import deque
from collections.abc import Sequence
from datetime import datetime

from .model import StoredTelegram
from .query import TelegramQuery, TelegramQueryResult
from .store import StoreCapabilities, TelegramStore

_LOGGER = logging.getLogger(__name__)


class BufferedTelegramStore:
    """Write-buffering wrapper for any TelegramStore backend.

    - store() is synchronous — appends to an in-memory buffer (no I/O).
    - The buffer is flushed to the inner store periodically via store_many().
    - store_many() passes directly through to the inner store (no buffering).
    - On flush failure, the batch is re-prepended to preserve data.
    - All read/query/eviction methods delegate directly to the inner store.
    """

    def __init__(
        self,
        inner: TelegramStore,
        *,
        flush_interval: float = 1.0,
    ) -> None:
        """Initialize the buffered store."""
        self.inner = inner
        self.flush_interval = flush_interval
        self._buffer: deque[StoredTelegram] = deque()
        self._flush_task: asyncio.Task | None = None
        self._closing = False

    @property
    def capabilities(self) -> StoreCapabilities:
        """Return the capabilities of the inner store."""
        return self.inner.capabilities

    @property
    def retention_days(self) -> int | None:
        """Return the configured retention period of the inner store."""
        return self.inner.retention_days

    @property
    def max_telegrams(self) -> int | None:
        """Return the configured maximum number of telegrams of the inner store."""
        return self.inner.max_telegrams

    def start(self) -> None:
        """Start the periodic flush task."""
        if self._flush_task is not None:
            return
        self._flush_task = asyncio.create_task(self._flush_loop())

    async def stop(self) -> None:
        """Stop the periodic flush task and perform a final flush."""
        self._closing = True
        if self._flush_task is not None:
            self._flush_task.cancel()
            try:
                await self._flush_task
            except asyncio.CancelledError:
                pass
            self._flush_task = None

        await self.flush()
        await self.inner.close()

    def store(self, telegram: StoredTelegram) -> None:
        """Append a telegram to the buffer (synchronous)."""
        if self._closing:
            _LOGGER.warning("Store is closing, dropping telegram")
            return
        self._buffer.append(telegram)

    async def store_many(self, telegrams: Sequence[StoredTelegram]) -> None:
        """Persist multiple telegrams directly to the inner store (bypass buffer)."""
        await self.inner.store_many(telegrams)

    async def flush(self) -> None:
        """Flush the buffer to the inner store."""
        if not self._buffer:
            return

        # Take everything from the buffer
        batch: list[StoredTelegram] = []
        while self._buffer:
            batch.append(self._buffer.popleft())

        try:
            await self.inner.store_many(batch)
        except Exception as err:
            _LOGGER.error("Error flushing telegram buffer: %s", err)
            # Re-prepend the batch to the buffer
            self._buffer.extendleft(reversed(batch))

    async def _flush_loop(self) -> None:
        """Periodic flush loop."""
        while not self._closing:
            try:
                await asyncio.sleep(self.flush_interval)
                await self.flush()
            except asyncio.CancelledError:
                break
            except Exception as err:
                _LOGGER.exception("Unexpected error in flush loop: %s", err)

    async def initialize(self) -> None:
        """Initialize the inner store."""
        await self.inner.initialize()

    async def query(self, query: TelegramQuery) -> TelegramQueryResult:
        """Query the inner store."""
        return await self.inner.query(query)

    async def count(self) -> int:
        """Return the total number of stored telegrams in the inner store."""
        return await self.inner.count()

    async def evict_older_than(self, cutoff: datetime, *, dry_run: bool = False) -> int:
        """Evict telegrams from the inner store."""
        return await self.inner.evict_older_than(cutoff, dry_run=dry_run)

    async def evict_expired(self, *, dry_run: bool = False) -> int:
        """Evict expired telegrams from the inner store."""
        return await self.inner.evict_expired(dry_run=dry_run)

    async def clear(self) -> None:
        """Clear the inner store and the buffer."""
        self._buffer.clear()
        await self.inner.clear()
