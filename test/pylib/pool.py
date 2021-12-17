import asyncio
from typing import Generic, Callable, Awaitable, TypeVar, AsyncContextManager

T = TypeVar('T')


class Pool(Generic[T]):
    """Asynchronous object pool.
    You need a pool of up to N objects, but objects should be created
    on demand, so that if you use less, you don't create anything upfront.
    If there is no object in the pool and all N objects are in use, you want
    to wait until one of the object is returned to the pool. Expects a
    builder async function to build a new object.

    Usage example:
    async def start_server():
        return Server()
    pool = Pool(4, start_server)
    ...
    async with pool.instance() as server:
        await run_test(test, server)
    """

    def __init__(self, size: int, build: Callable[[], Awaitable[T]]):
        assert(size >= 0)
        self.pool: asyncio.Queue[T] = asyncio.Queue(size)
        self.build = build
        self.total = 0

    async def get(self) -> T:
        if self.pool.empty() and self.total < self.pool.maxsize:
            # Increment the total first to avoid a race
            # during self.build()
            self.total += 1
            try:
                await self.pool.put(await self.build())
            except:     # noqa: E722
                self.total -= 1
                raise

        return await self.pool.get()

    async def put(self, obj: T):
        await self.pool.put(obj)

    def instance(self) -> AsyncContextManager[T]:
        class Instance:
            def __init__(self, pool):
                self.pool = pool

            async def __aenter__(self):
                self.obj = await self.pool.get()
                return self.obj

            async def __aexit__(self, exc_type, exc, obj):
                if self.obj:
                    await self.pool.put(self.obj)
                    self.obj = None

        return Instance(self)
