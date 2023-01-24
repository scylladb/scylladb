import asyncio
from typing import Generic, Callable, Awaitable, TypeVar, AsyncContextManager, Final

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

        server = await pool.get()
        try:
            await run_test(test, server)
        finally:
            await pool.put(server)

    Alternatively:
        async with pool.instance() as server:
            await run_test(test, server)

    If the object is considered no longer usable by other users of the pool
    you can 'steal' it, which frees up space in the pool.
        server = await.pool.get()
        dirty = True
        try:
            dirty = await run_test(test, server)
        finally:
            if dirty:
                await pool.steal()
            else:
                await pool.put(server)
    """
    def __init__(self, max_size: int, build: Callable[..., Awaitable[T]]):
        assert(max_size >= 0)
        self.max_size: Final[int] = max_size
        self.build: Final[Callable[..., Awaitable[T]]] = build
        self.cond: Final[asyncio.Condition] = asyncio.Condition()
        self.pool: list[T] = []
        self.total: int = 0 # len(self.pool) + leased objects

    async def get(self, *args, **kwargs) -> T:
        """Borrow an object from the pool.

           If a new object must be built first, *args and **kwargs
           will be passed to the build function and the object built
           in this way will be returned. However, remember that there
           is no guarantee whether a new object will be built
           or an existing one will be borrowed.
        """
        async with self.cond:
            await self.cond.wait_for(lambda: self.pool or self.total < self.max_size)
            if self.pool:
                return self.pool.pop()

            # No object in pool, but total < max_size so we can construct one
            self.total += 1

        try:
            obj = await self.build(*args, **kwargs)
        except:
            async with self.cond:
                self.total -= 1
                self.cond.notify()
            raise
        return obj

    async def steal(self) -> None:
        """Take ownership of a previously borrowed object.
           Frees up space in the pool.
        """
        async with self.cond:
            self.total -= 1
            self.cond.notify()

    async def put(self, obj: T):
        """Return a previously borrowed object to the pool."""
        async with self.cond:
            self.pool.append(obj)
            self.cond.notify()

    def instance(self, *args, **kwargs) -> AsyncContextManager[T]:
        class Instance:
            def __init__(self, pool):
                self.pool = pool

            async def __aenter__(self):
                self.obj = await self.pool.get(*args, **kwargs)
                return self.obj

            async def __aexit__(self, exc_type, exc, obj):
                if self.obj:
                    await self.pool.put(self.obj)
                    self.obj = None

        return Instance(self)
