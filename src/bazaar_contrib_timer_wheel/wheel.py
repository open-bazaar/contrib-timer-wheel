from __future__ import annotations

import asyncio
import time
from dataclasses import dataclass, replace
from enum import Enum, auto
from typing import Annotated

from bazaar_contrib_timer import TimerCancelledError, TimerClosedError
from pydantic import Field, TypeAdapter

_NEAR_BITS = 8
_NEAR_SIZE = 1 << _NEAR_BITS
_LEVEL_BITS = 6
_LEVEL_SIZE = 1 << _LEVEL_BITS
_LEVEL_COUNT = 4
_MAX_DELAY_TICKS = (1 << (_NEAR_BITS + _LEVEL_BITS * _LEVEL_COUNT)) - 1


_TICK_MS = 10
_DELAY = TypeAdapter(
    Annotated[int, Field(strict=True, ge=0, le=_MAX_DELAY_TICKS * _TICK_MS)]
)


@dataclass(slots=True)
class _TimerEntry:
    timer: _Timer
    deadline_tick: int
    generation: int
    level: int
    slot: int


@dataclass(frozen=True, slots=True)
class _Pending:
    pass


@dataclass(frozen=True, slots=True)
class _Expired:
    generation: int


@dataclass(frozen=True, slots=True)
class _Consumed(_Expired):
    pass


@dataclass(frozen=True, slots=True)
class _Cancelled:
    expiry: _Expired | None = None


@dataclass(frozen=True, slots=True)
class _Closed:
    expiry: _Expired | None = None


class _WheelState(Enum):
    OPEN = auto()
    CLOSED = auto()


class _Timer:
    """One resettable, single-consumer timer owned by a TimerWheel."""

    def __init__(
        self,
        wheel: TimerWheel,
        timer_id: int,
        deadline_tick: int,
    ) -> None:
        self._wheel = wheel
        self._id = timer_id
        self._deadline_tick = deadline_tick
        self._generation = 1
        self._state: _Pending | _Expired | _Consumed | _Cancelled | _Closed = _Pending()
        self._waiter: asyncio.Future[None] | None = None

    @property
    def id(self) -> int:
        return self._id

    @property
    def active(self) -> bool:
        return isinstance(self._state, _Pending)

    @property
    def deadline(self) -> int:
        return self._deadline_tick * self._wheel.tick_ms

    @property
    def generation(self) -> int:
        return self._generation

    @property
    def expired_generation(self) -> int | None:
        state = self._state
        expiry = state.expiry if isinstance(state, (_Cancelled, _Closed)) else state
        return expiry.generation if isinstance(expiry, _Expired) else None

    def reset(self, delay_ms: int) -> None:
        if not isinstance(self._state, _Pending):
            raise RuntimeError("only an active timer can be reset")  # noqa: TRY004
        self._wheel._reschedule(self, _DELAY.validate_python(delay_ms))

    def cancel(self) -> None:
        state = self._state
        if isinstance(state, (_Cancelled, _Closed)):
            return
        if isinstance(state, _Pending):
            self._wheel._remove(self)
        self._state = _Cancelled(state if isinstance(state, _Expired) else None)
        waiter = self._waiter
        if waiter is not None and not waiter.done():
            waiter.set_exception(TimerCancelledError("timer was cancelled"))

    async def wait(self) -> None:
        if self._waiter is not None:
            raise RuntimeError("timer already has a waiter")
        state = self._state
        if isinstance(state, _Closed):
            raise TimerClosedError("timer wheel is closed")
        if isinstance(state, _Cancelled):
            raise TimerCancelledError("timer was cancelled")
        if isinstance(state, _Consumed):
            raise RuntimeError("timer expiry was already consumed")  # noqa: TRY004
        if isinstance(state, _Expired):
            self._state = _Consumed(state.generation)
            return
        waiter = asyncio.get_running_loop().create_future()
        self._waiter = waiter
        try:
            await waiter
            state = self._state
            if isinstance(state, _Expired):
                self._state = _Consumed(state.generation)
            elif isinstance(state, (_Cancelled, _Closed)) and state.expiry is not None:
                self._state = replace(state, expiry=_Consumed(state.expiry.generation))
        except BaseException:
            if isinstance(self._state, _Pending):
                self.cancel()
            raise
        finally:
            self._waiter = None

    def _expire(self, generation: int) -> None:
        if not isinstance(self._state, _Pending) or generation != self._generation:
            return
        self._state = _Expired(generation)
        waiter = self._waiter
        if waiter is not None and not waiter.done():
            waiter.set_result(None)

    def _close(self) -> None:
        state = self._state
        expiry = state.expiry if isinstance(state, (_Cancelled, _Closed)) else state
        self._state = _Closed(expiry if isinstance(expiry, _Expired) else None)
        waiter = self._waiter
        if waiter is not None and not waiter.done():
            waiter.set_exception(TimerClosedError("timer wheel is closed"))


class TimerWheel:
    """A process-local hierarchical timing wheel driven by one asyncio task."""

    def __init__(self) -> None:
        self._tick_ms = _TICK_MS
        self._current_tick = time.monotonic_ns() // (self._tick_ms * 1_000_000)
        self._near: list[dict[int, _TimerEntry]] = [{} for _ in range(_NEAR_SIZE)]
        self._levels: list[list[dict[int, _TimerEntry]]] = [
            [{} for _ in range(_LEVEL_SIZE)] for _ in range(_LEVEL_COUNT)
        ]
        self._entries: dict[int, _TimerEntry] = {}
        self._next_timer_id = 1
        self._poke = asyncio.Event()
        self._state = _WheelState.OPEN

    @property
    def tick_ms(self) -> int:
        return self._tick_ms

    @property
    def maximum_delay_ms(self) -> int:
        return _MAX_DELAY_TICKS * self._tick_ms

    @property
    def now_ms(self) -> int:
        return time.monotonic_ns() // 1_000_000

    def close(self) -> None:
        if self._state is _WheelState.CLOSED:
            return
        self._state = _WheelState.CLOSED
        entries = tuple(self._entries.values())
        self._entries.clear()
        for bucket in self._near:
            bucket.clear()
        for level in self._levels:
            for bucket in level:
                bucket.clear()
        for entry in entries:
            entry.timer._close()

    def create(self, delay_ms: int) -> _Timer:
        delay_ms = _DELAY.validate_python(delay_ms)
        self._require_open()
        deadline_tick = self._deadline(delay_ms)
        timer = _Timer(self, self._next_timer_id, deadline_tick)
        self._next_timer_id += 1
        self._insert(timer, deadline_tick, timer.generation)
        self._poke.set()
        return timer

    async def run(self) -> None:
        try:
            while True:
                if not self._entries:
                    self._poke.clear()
                    await self._poke.wait()
                    continue
                await asyncio.sleep(self._tick_ms / 1_000)
                self._advance_to(time.monotonic_ns() // (self._tick_ms * 1_000_000))
        except asyncio.CancelledError as error:
            if self._state is not _WheelState.CLOSED:
                raise RuntimeError(
                    "timer wheel driver was cancelled unexpectedly"
                ) from error
            raise
        finally:
            self.close()

    def _advance_to(self, target_tick: int) -> None:
        elapsed = target_tick - self._current_tick
        if elapsed <= 0:
            return
        if elapsed >= _NEAR_SIZE and elapsed > len(self._entries):
            self._rebuild(target_tick)
            return
        while self._current_tick < target_tick:
            self._current_tick += 1
            if self._current_tick & (_NEAR_SIZE - 1) == 0:
                self._cascade(0)
            self._expire_near(self._current_tick & (_NEAR_SIZE - 1))

    def _rebuild(self, target_tick: int) -> None:
        entries = tuple(self._entries.values())
        self._entries.clear()
        for bucket in self._near:
            bucket.clear()
        for level in self._levels:
            for bucket in level:
                bucket.clear()
        self._current_tick = target_tick
        for entry in entries:
            if entry.deadline_tick <= target_tick:
                entry.timer._expire(entry.generation)
            else:
                self._insert(entry.timer, entry.deadline_tick, entry.generation)

    def _cascade(self, level_index: int) -> None:
        shift = _NEAR_BITS + level_index * _LEVEL_BITS
        slot = (self._current_tick >> shift) & (_LEVEL_SIZE - 1)
        bucket = self._levels[level_index][slot]
        entries = tuple(bucket.values())
        bucket.clear()
        for entry in entries:
            self._entries.pop(entry.timer.id, None)
            self._insert(entry.timer, entry.deadline_tick, entry.generation)
        if slot == 0 and level_index + 1 < _LEVEL_COUNT:
            self._cascade(level_index + 1)

    def _expire_near(self, slot: int) -> None:
        bucket = self._near[slot]
        entries = tuple(bucket.values())
        bucket.clear()
        for entry in entries:
            self._entries.pop(entry.timer.id, None)
            if entry.deadline_tick <= self._current_tick:
                entry.timer._expire(entry.generation)
            else:
                self._insert(entry.timer, entry.deadline_tick, entry.generation)

    def _reschedule(self, timer: _Timer, delay_ms: int) -> None:
        self._require_open()
        deadline_tick = self._deadline(delay_ms)
        if not timer.active:
            raise RuntimeError("only an active timer can be reset")
        self._remove(timer)
        timer._generation += 1
        timer._deadline_tick = deadline_tick
        self._insert(timer, timer._deadline_tick, timer.generation)

    def _insert(self, timer: _Timer, deadline_tick: int, generation: int) -> None:
        delta = deadline_tick - self._current_tick
        if delta < _NEAR_SIZE:
            level = -1
            slot = deadline_tick & (_NEAR_SIZE - 1)
            bucket = self._near[slot]
        else:
            level = min(
                (delta.bit_length() - _NEAR_BITS - 1) // _LEVEL_BITS,
                _LEVEL_COUNT - 1,
            )
            shift = _NEAR_BITS + level * _LEVEL_BITS
            slot = (deadline_tick >> shift) & (_LEVEL_SIZE - 1)
            bucket = self._levels[level][slot]
        entry = _TimerEntry(timer, deadline_tick, generation, level, slot)
        bucket[timer.id] = entry
        self._entries[timer.id] = entry

    def _remove(self, timer: _Timer) -> None:
        entry = self._entries.pop(timer.id, None)
        if entry is None:
            return
        if entry.level == -1:
            self._near[entry.slot].pop(timer.id, None)
        else:
            self._levels[entry.level][entry.slot].pop(timer.id, None)

    def _deadline(self, delay_ms: int) -> int:
        self._advance_to(time.monotonic_ns() // (self._tick_ms * 1_000_000))
        return self._current_tick + max(
            1, (delay_ms + self._tick_ms - 1) // self._tick_ms
        )

    def _require_open(self) -> None:
        if self._state is _WheelState.CLOSED:
            raise TimerClosedError("timer wheel is closed")
