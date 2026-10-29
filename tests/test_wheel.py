from __future__ import annotations

lazy import asyncio
lazy import time
lazy from collections.abc import AsyncIterator
lazy from contextlib import asynccontextmanager
lazy from pathlib import Path

import pytest
from bazaar_contrib_timer import (
    TIMER,
    Timer,
    TimerCancelledError,
    TimerClosedError,
    timeout,
    wait_for,
)
from bazaar_core import (
    CONTRIB_CHANGED,
    Blocked,
    Composition,
    Context,
    Contrib,
    ContribChange,
    ContribEntry,
    ContribHub,
    Failed,
)
lazy import aiofiles

from bazaar_contrib_timer_wheel import timer_wheel


@pytest.fixture
async def timer() -> AsyncIterator[Timer]:
    services: list[Timer] = []

    @asynccontextmanager
    async def consumer(ctx: Context):
        services.append(ctx.accept(TIMER))
        yield

    async with (
        ContribHub() as hub,
        hub.enable(
            Composition(
                entries=[
                    ContribEntry(name="wheel", definition=timer_wheel),
                    ContribEntry(
                        name="consumer",
                        definition=Contrib(accepts=[TIMER], lifespan=consumer),
                    ),
                ],
                roots=("consumer",),
            )
        ),
    ):
        yield services[0]


async def test_expiry_and_reset_keep_the_waiter_and_respect_elapsed_time(timer: Timer):
    await timer.create(0).wait()
    handle = timer.create(timer.maximum_delay_ms)
    generation = handle.generation
    waiter = asyncio.create_task(handle.wait(), eager_start=True)
    started = time.monotonic_ns()
    handle.reset(47)
    await waiter
    assert time.monotonic_ns() - started + timer.tick_ms * 1_000_000 >= 47_000_000
    assert handle.generation > generation
    assert handle.expired_generation == handle.generation
    assert timer.now_ms >= handle.deadline


async def test_concurrent_timers_cross_the_near_ring_and_resume_from_idle(
    timer: Timer,
):
    handles = [timer.create(delay) for delay in range(0, 150, 3)]
    handles.append(timer.create(timer.tick_ms * 257))
    await asyncio.gather(*(handle.wait() for handle in handles))
    assert all(handle.expired_generation is not None for handle in handles)
    await asyncio.sleep(0.03)
    await timer.create(0).wait()


async def test_cancel_and_waiter_cancellation_release_long_lived_handles(timer: Timer):
    for delay in (1 << 14, 1 << 20, 1 << 26):
        handle = timer.create(delay * timer.tick_ms)
        waiter = asyncio.create_task(handle.wait(), eager_start=True)
        handle.cancel()
        handle.cancel()
        with pytest.raises(TimerCancelledError):
            await waiter

    handle = timer.create(timer.maximum_delay_ms)
    waiter = asyncio.create_task(handle.wait(), eager_start=True)
    waiter.cancel()
    with pytest.raises(asyncio.CancelledError):
        await waiter
    with pytest.raises(TimerCancelledError):
        await handle.wait()
    await timer.create(0).wait()


async def test_nested_consumers_share_a_provider_and_release_before_its_shutdown():
    services: list[Timer] = []
    released = asyncio.Event()

    @asynccontextmanager
    async def child(ctx: Context):
        service = ctx.accept(TIMER)
        services.append(service)
        handle = service.create(service.maximum_delay_ms)
        try:
            yield
        finally:
            handle.cancel()
            with pytest.raises(TimerCancelledError):
                await handle.wait()
            await service.create(0).wait()
            released.set()

    @asynccontextmanager
    async def parent(ctx: Context):
        services.append(ctx.accept(TIMER))
        async with ctx.enable(
            Composition(
                entries=[
                    ContribEntry(
                        name="child",
                        definition=Contrib(accepts=[TIMER], lifespan=child),
                    )
                ]
            )
        ):
            yield

    async with (
        ContribHub() as hub,
        hub.enable(
            Composition(
                entries=[
                    ContribEntry(name="wheel", definition=timer_wheel),
                    ContribEntry(
                        name="parent",
                        definition=Contrib(accepts=[TIMER], lifespan=parent),
                    ),
                ],
                roots=("parent",),
            )
        ),
    ):
        assert services[0] is services[1]
    assert released.is_set()


async def test_provider_shutdown_completes_pending_waiters_with_the_shared_error():
    services: list[Timer] = []

    @asynccontextmanager
    async def consumer(ctx: Context):
        services.append(ctx.accept(TIMER))
        yield

    async def guarded_work():
        async with timeout(services[0], 60_000):
            await asyncio.Event().wait()

    async with (
        ContribHub() as hub,
        hub.enable(
            Composition(
                entries=[
                    ContribEntry(name="wheel", definition=timer_wheel),
                    ContribEntry(
                        name="consumer",
                        definition=Contrib(accepts=[TIMER], lifespan=consumer),
                    ),
                ],
                roots=("consumer",),
            )
        ),
    ):
        handle = services[0].create(60_000)
        waiter = asyncio.create_task(handle.wait(), eager_start=True)
        guarded = asyncio.create_task(guarded_work(), eager_start=True)
    with pytest.raises(TimerClosedError):
        await waiter
    with pytest.raises(TimerClosedError):
        await handle.wait()
    with pytest.raises(TimerClosedError):
        await guarded


async def test_unexpected_driver_cancellation_closes_handles_and_blocks_consumers():
    services: list[Timer] = []
    failed = asyncio.Event()
    released = asyncio.Event()

    @asynccontextmanager
    async def consumer(ctx: Context):
        services.append(ctx.accept(TIMER))
        try:
            yield
        finally:
            released.set()

    def changed(ctx: Context, change: ContribChange):
        if any(
            item.name == "wheel" and isinstance(item.state, Failed)
            for item in change.snapshots
        ):
            failed.set()

    async with (
        ContribHub() as hub,
        hub.enable(
            Composition(
                entries=[
                    ContribEntry(name="wheel", definition=timer_wheel),
                    ContribEntry(
                        name="consumer",
                        definition=Contrib(accepts=[TIMER], lifespan=consumer),
                    ),
                    ContribEntry(
                        name="observer",
                        definition=Contrib(listen={CONTRIB_CHANGED: changed}),
                    ),
                ],
                roots=("consumer", "observer"),
            )
        ),
    ):
        handle = services[0].create(60_000)
        waiter = asyncio.create_task(handle.wait(), eager_start=True)
        provider = next(item for item in hub.snapshots if item.name == "wheel")
        driver = next(
            task
            for task in asyncio.all_tasks()
            if task.get_name() == f"timer-wheel:{provider.id}"
        )
        driver.cancel()
        with pytest.raises(TimerClosedError):
            await waiter
        await failed.wait()
        await released.wait()
        assert driver.done()
        assert any(
            item.name == "consumer" and isinstance(item.state, Blocked)
            for item in hub.snapshots
        )


async def test_timeout_allows_completed_writes_and_later_work(
    timer: Timer, tmp_path: Path
):
    path = tmp_path / "completed.txt"
    async with aiofiles.open(path, "w") as stream:
        async with timeout(timer, 200):
            await stream.write("message saved\n")
        await timer.create(300).wait()
        await stream.write("later message saved\n")
    async with aiofiles.open(path) as stream:
        content = await wait_for(timer, stream.read(), 200)
    assert "message saved" in content and "later message saved" in content


@pytest.mark.parametrize("delay_ms", [-1, 0, 30])
async def test_timeout_runs_fallback_and_allows_the_next_operation(
    timer: Timer, tmp_path: Path, delay_ms: int
):
    path = tmp_path / "fallback.txt"
    async with aiofiles.open(path, "w") as stream:
        try:
            await wait_for(timer, timer.create(5_000).wait(), delay_ms)
        except TimeoutError:
            await stream.write("fallback delivered\n")
        await wait_for(timer, stream.write("next operation completed\n"), None)
    async with aiofiles.open(path) as stream:
        content = await stream.read()
    assert "fallback delivered" in content and "next operation completed" in content


async def test_timeout_preserves_external_cancellation_and_file_cleanup(
    timer: Timer, tmp_path: Path
):
    path = tmp_path / "cancelled.txt"
    entered = asyncio.Event()

    async def write():
        async with aiofiles.open(path, "w") as stream, timeout(timer, 2_000):
            try:
                entered.set()
                await timer.create(5_000).wait()
            finally:
                await stream.write("request resources released\n")

    request = asyncio.create_task(wait_for(timer, write(), 2_000))
    await entered.wait()
    request.cancel()
    with pytest.raises(asyncio.CancelledError):
        await request
    async with aiofiles.open(path) as stream:
        assert "request resources released" in await stream.read()


@pytest.mark.parametrize("delay_ms", [0, 30])
async def test_nested_timeouts_allow_inner_and_outer_business_fallbacks(
    timer: Timer, tmp_path: Path, delay_ms: int
):
    path = tmp_path / "nested.txt"
    async with aiofiles.open(path, "w") as stream:
        async with timeout(timer, 2_000):
            try:
                async with timeout(timer, delay_ms):
                    await timer.create(5_000).wait()
            except TimeoutError:
                await stream.write("inner fallback delivered\n")
            await stream.write("outer operation continued\n")
        try:
            async with timeout(timer, delay_ms):
                try:
                    async with timeout(timer, 2_000):
                        await timer.create(5_000).wait()
                except TimeoutError:
                    await stream.write("inner deadline handled\n")
        except TimeoutError:
            await stream.write("outer fallback delivered\n")
    async with aiofiles.open(path) as stream:
        content = await stream.read()
    assert all(
        message in content
        for message in (
            "inner fallback delivered",
            "outer operation continued",
            "outer fallback delivered",
        )
    )


async def test_external_cancellation_during_timeout_cleanup_is_preserved(
    timer: Timer, tmp_path: Path
):
    path = tmp_path / "interrupted-cleanup.txt"
    cleaning = asyncio.Event()
    released = asyncio.Event()

    async def write():
        async with aiofiles.open(path, "w") as stream, timeout(timer, 30):
            try:
                await timer.create(5_000).wait()
            finally:
                await stream.write("cleanup started\n")
                cleaning.set()
                await released.wait()

    request = asyncio.create_task(write())
    await cleaning.wait()
    request.cancel()
    released.set()
    with pytest.raises(asyncio.CancelledError):
        await request
    async with aiofiles.open(path) as stream:
        assert "cleanup started" in await stream.read()
