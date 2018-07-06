from asyncio_pool.async_pool import AsyncPoolExecutor
from asyncio_pool.exc import EventLoopStoppedError
import pytest
import asyncio
import multiprocessing

pytestmark = pytest.mark.asyncio


async def test_constructor():
    test_executor = AsyncPoolExecutor()
    assert type(test_executor) is AsyncPoolExecutor
    with pytest.raises(EventLoopStoppedError):
        test_executor = AsyncPoolExecutor(loop=asyncio.new_event_loop())
    test_executor = AsyncPoolExecutor(max_workers=0)
    assert test_executor.semaphore._value == multiprocessing.cpu_count() * 5
    test_executor = AsyncPoolExecutor(max_workers=30)
    assert test_executor.semaphore._value == 30


@pytest.mark.timeout(2)
async def test_submit():
    async with AsyncPoolExecutor() as pool:
        for _ in range(20):
            pool.submit(asyncio.sleep(1))


@pytest.mark.timeout(2)
async def test_map():
    async with AsyncPoolExecutor() as pool:
        pool.map(asyncio.sleep, [1] * 10)


@pytest.mark.timeout(1)
async def test_shutdown():
    pool = AsyncPoolExecutor()
    pool.map(asyncio.sleep, [5] * 1)
    await pool.shutdown(wait=False)


@pytest.mark.timeout(2)
async def test_wait():
    pool = AsyncPoolExecutor()
    pool.map(asyncio.sleep, [1] * 15)
    await pool.wait()


@pytest.mark.timeout(2)
async def test_initializer():
    async with AsyncPoolExecutor(initializer=print, initargs=('Initializing',)) as pool:
        pool.map(asyncio.sleep, [1] * 15)
