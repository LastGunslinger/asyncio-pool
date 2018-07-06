import asyncio
from asyncio import AbstractEventLoop
from multiprocessing import cpu_count
from typing import Any, Awaitable, Callable, Iterable, List, Optional, Tuple

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
        """Add a coroutine to the pool to be run in the background"""
        coro = self._wrap_in_semaphore(coro)
        task = asyncio.ensure_future(coro)
        self.pending_tasks.append(task)
        return task

    def map(self, func: Awaitable, *iterables: Iterable) -> List[Awaitable]:
        """Map the values in iterables to a function and add to the pool"""
        tasks = []
        for args in zip(*iterables):
            coro = self._wrap_in_semaphore(func(*args))
            tasks.append(self.submit(coro))
        return tasks

    async def shutdown(self, wait: bool=True) -> None:
        """Shutdown the pool and clear all pending tasks"""
        if wait:
            await self.wait()
        else:
            self.pending_tasks = [asyncio.sleep(0.0001)]

    async def wait(self, timeout=None, return_when: str=asyncio.FIRST_EXCEPTION) -> Tuple[Any]:
        """Wait for all pending tasks to complete"""
        if not self.pending_tasks:
            return self.completed_tasks, self.pending_tasks
        done, _ = await asyncio.wait(self.pending_tasks, loop=self.event_loop, timeout=timeout, return_when=return_when)
        for fut in done:
            self.pending_tasks.remove(fut)
            try:
                self.completed_tasks.append(await fut)
            except Exception as exc:
                self.errors.append(exc)
                raise
        return self.completed_tasks, self.pending_tasks

    def started(self) -> bool:
        """Check whether any tasks have been added to the pool yet"""
        if self.pending_tasks or self.completed_tasks:
            return True
        else:
            return False

    async def done(self, timeout: Optional[float]=None) -> bool:
        """Check whether all tasks have been completed"""
        if not self.started():
            return False
        elif self.completed_tasks and not self.pending_tasks:
            return True

        try:
            done, pending = await self.wait(
                timeout=timeout,
                return_when=asyncio.FIRST_COMPLETED
            )
        except asyncio.TimeoutError:
            raise
        if pending:
            return False
        else:
            return True

    def _wrap_in_semaphore(self, coro: Awaitable) -> Awaitable:
        """Wrap the given coroutine in a semaphore context"""
        async def wrapper(coro: Awaitable):
            async with self.semaphore:
                if not self.initializer:
                    pass
                elif asyncio.iscoroutinefunction(self.initializer):
                    await self.initializer(*self.initargs)
                else:
                    self.initializer(*self.initargs)
                return await coro
        return wrapper(coro)
