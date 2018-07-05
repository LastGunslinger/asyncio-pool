import asyncio
from multiprocessing import cpu_count
from asyncio import AbstractEventLoop
from typing import Awaitable, Callable, Optional, Tuple
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
            raise EventLoopStoppedError('AsyncPoolExecutor can only be used in the context of a running asyncio event loop')
        self.semaphore = asyncio.BoundedSemaphore(max_workers or cpu_count() * 5)
        self.name_prefix = coro_name_prefix
        self.initializer = initializer
        self.initargs = initargs
        self.tasks = []

    def submit(self, *coros: Awaitable) -> Awaitable:
        current_task_count = len(self.tasks)
        for coro in coros:
            coro = self._wrap_in_semaphore(coro)
            self.tasks.append(asyncio.ensure_future(coro))
        return self.tasks[current_task_count:]

    def started(self) -> bool:
        if self.tasks:
            return True
        else:
            return False

    async def done(self) -> bool:
        if not self.tasks:
            raise Exception('No tasks have been started.')
        done, pending = await asyncio.wait(self.tasks, return_when=asyncio.FIRST_COMPLETED)
        if pending:
            return False
        else:
            return True

    async def wait(self) -> Tuple[list]:
        done, pending = await asyncio.wait(self.tasks)
        return done, pending

    async def reset(self) -> None:
        if self.tasks:
            await self.wait()
        self.tasks = []

    def _wrap_in_semaphore(self, coro: Awaitable) -> Awaitable:
        async def wrapper(coro: Awaitable):
            async with self.semaphore:
                return await coro
        return wrapper(coro)