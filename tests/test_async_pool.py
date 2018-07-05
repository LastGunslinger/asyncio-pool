from asyncio_pool import async_pool
from asyncio_pool.exc import EventLoopStoppedError
import pytest
import asyncio
import multiprocessing

pytestmark = pytest.mark.asyncio


async def test_constructor():
    test_executor = async_pool.AsyncPoolExecutor()
    assert type(test_executor) is async_pool.AsyncPoolExecutor
    with pytest.raises(EventLoopStoppedError):
        test_executor = async_pool.AsyncPoolExecutor(loop=asyncio.new_event_loop())
    test_executor = async_pool.AsyncPoolExecutor(max_workers=0)
    assert test_executor.semaphore._value == multiprocessing.cpu_count() * 5
    test_executor = async_pool.AsyncPoolExecutor(max_workers=30)
    assert test_executor.semaphore._value == 30


