"""Provide the shared timer service with a lifespan-owned timing wheel."""

from contextlib import asynccontextmanager
lazy import asyncio

from bazaar_contrib_timer import TIMER
from bazaar_core import Context, Contrib

lazy from .wheel import TimerWheel

__all__ = ["timer_wheel"]


@asynccontextmanager
async def lifespan(ctx: Context):
    wheel = TimerWheel()
    async with asyncio.TaskGroup() as tasks:
        tasks.create_task(wheel.run(), name=f"timer-wheel:{ctx.id}", eager_start=True)
        try:
            ctx.offer(TIMER, wheel)
            yield
        finally:
            wheel.close()
            tasks.cancel()


timer_wheel = Contrib(offers=[TIMER], lifespan=lifespan)
