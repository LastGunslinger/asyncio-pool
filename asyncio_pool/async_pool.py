import asyncio
from multiprocessing import cpu_count
from asyncio import AbstractEventLoop
from typing import Awaitable, Callable, Iterable, Optional, Tuple
from .exc import EventLoopStoppedError


class AsyncPoolExecutor():
    def __init__(self, loop: Optional[AbstractEventLoop]=None, max_workers: int=0, coro_name_prefix: str='', initializer: Optional[Callable]=None, initargs: Tuple=()):
        """Initialize a new AsyncPoolExecutor
        
        Keyword arguments:
        loop -- the event loop that the executor will use
        max_workers -- the maximum number of async workers that are allowed to be run concurrently
        coro_name_prefix -- name prefix to add to the async tasks that allows users to know which executor launched them
        initializer -- an optional callable that is called at the start of each worker process
        initargs -- a tuple of arguments passed to the initializer
        """
        self.event_loop = loop or asyncio.get_event_loop()
        if not self.event_loop.is_running():
            raise EventLoopStoppedError('AsyncPoolExecutor can only be used in the context of a running event loop')
        self.semaphore = asyncio.BoundedSemaphore(max_workers or cpu_count() * 5)
        self.name_prefix = coro_name_prefix
        self.initializer = initializer
        self.initargs = initargs
        self.pending_tasks = []
        self.completed_tasks = []
        self.errors = []

    async def __aenter__(self):
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        return await self.wait()

    def submit(self, coro: Awaitable) -> Awaitable:
        """Add a subroutine to the pool"""
        coro = self._wrap_in_semaphore(coro)
        task = asyncio.ensure_future(coro)
        self.pending_tasks.append(task)
        return task

    def map(self, func: Awaitable, *iterables: Iterable):
        for args in zip(*iterables):
            coro = self._wrap_in_semaphore(func(*args))
            self.submit(coro)

    async def shutdown(self, wait: bool=True):
        if wait:
            await self.wait()
        else:
            self.pending_tasks = [asyncio.sleep(0.0)]

    async def wait(self) -> Tuple[list]:
        done, _ = await asyncio.wait(self.pending_tasks, return_when=asyncio.FIRST_EXCEPTION)
        for fut in done:
            self.pending_tasks.remove(fut)
            try:
                self.completed_tasks.append(await fut)
            except Exception as exc:
                self.errors.append(exc)
                raise
        return self.completed_tasks

    def started(self) -> bool:
        if self.pending_tasks or self.completed_tasks:
            return True
        else:
            return False

    async def done(self) -> bool:
        if not self.started():
            raise Exception('No tasks have been started.')
        elif self.pending_tasks:
            return False
        else:
            return True

    async def reset(self) -> None:
        if self.tasks:
            await self.wait()
        self.tasks = []

    def _wrap_in_semaphore(self, coro: Awaitable) -> Awaitable:
        """Wrap the given coroutine in a semaphore context"""
        async def wrapper(coro: Awaitable):
            async with self.semaphore:
                if asyncio.iscoroutinefunction(self.initializer):
                    await self.initializer(*self.initargs)
                else:
                    self.initializer(*self.initargs)
                return await coro
        return wrapper(coro)