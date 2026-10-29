"""Consume the installed time wheel solely through the shared timer contract."""

import asyncio
from contextlib import asynccontextmanager
from importlib.metadata import entry_points

from bazaar_contrib_timer import TIMER, Timer, TimerClosedError
from bazaar_core import Composition, Context, Contrib, ContribEntry, ContribHub


async def main():
    services: list[Timer] = []

    @asynccontextmanager
    async def consumer(ctx: Context):
        service = ctx.accept(TIMER)
        services.append(service)
        await service.create(0).wait()
        yield

    entries = entry_points(group="bazaar_compute_node.contribs", name="timer-wheel")
    definition = next(iter(entries)).load()
    async with (
        ContribHub() as hub,
        hub.enable(
            Composition(
                entries=[
                    ContribEntry(name="wheel", definition=definition),
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
        handle.reset(20)
        await handle.wait()
        handle = services[0].create(60_000)
    try:
        await handle.wait()
    except TimerClosedError:
        return
    raise RuntimeError("installed timer provider did not close its pending handle")


if __name__ == "__main__":
    asyncio.run(main())
