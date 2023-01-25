import asyncio
from typing import Generic, Callable, Awaitable, TypeVar, AsyncContextManager, Final, Optional

T = TypeVar('T')


class Pool(Generic[T]):
    """Asynchronous object pool.
    You need a pool of up to N objects, but objects should be created
    on demand, so that if you use less, you don't create anything upfront.
    If there is no object in the pool and all N objects are in use, you want
    to wait until one of the object is returned to the pool. Expects a
    builder async function to build a new object and a destruction async
    function to clean up after a 'dirty' object (see below).

    Usage example:
        async def start_server():
            return Server()
        async def destroy_server(server):
            await server.free_resources()
        pool = Pool(4, start_server, destroy_server)

        server = await pool.get()
        try:
            await run_test(test, server)
        finally:
            await pool.put(server)

    Alternatively:
        async with pool.instance(dirty_on_exception=False) as server:
            await run_test(test, server)


    If the object is considered no longer usable by other users of the pool
    you can pass `is_dirty=True` flag to `put`, which will cause the object
    to be 'destroyed' (by calling the provided `destroy` function on it) and
    will free up space in the pool.
        server = await.pool.get()
        dirty = True
        try:
            dirty = await run_test(test, server)
        finally:
            await pool.put(server, is_dirty=dirty)

    Alternatively:
        async with (cm := pool.instance(dirty_on_exception=True)) as server:
            cm.dirty = await run_test(test, server)
            # It will also be considered dirty if run_test throws an exception
    """
    def __init__(self, max_size: int,
                 build: Callable[..., Awaitable[T]],
                 destroy: Callable[[T], Awaitable[None]]):
        assert(max_size >= 0)
        self.max_size: Final[int] = max_size
        self.build: Final[Callable[..., Awaitable[T]]] = build
        self.destroy: Final[Callable[[T], Awaitable]] = destroy
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

    async def put(self, obj: T, is_dirty: bool):
        """Return a previously borrowed object to the pool
           if it's not dirty, otherwise destroy the object
           and free up space in the pool.
        """
        if is_dirty:
            await self.destroy(obj)

        async with self.cond:
            if is_dirty:
                self.total -= 1
            else:
                self.pool.append(obj)
            self.cond.notify()

    def instance(self, dirty_on_exception: bool, *args, **kwargs) -> AsyncContextManager[T]:
        class Instance:
            def __init__(self, pool: Pool[T], dirty_on_exception: bool):
                self.pool = pool
                self.dirty = False
                self.dirty_on_exception = dirty_on_exception

            async def __aenter__(self):
                self.obj = await self.pool.get(*args, **kwargs)
                return self.obj

            async def __aexit__(self, exc_type, exc, obj):
                if self.obj:
                    self.dirty |= self.dirty_on_exception and exc is not None
                    await self.pool.put(self.obj, is_dirty=self.dirty)
                    self.obj = None

        return Instance(self, dirty_on_exception)
