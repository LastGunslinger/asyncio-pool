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


@pytest.mark.timeout(2)
async def test_submit():
    async with async_pool.AsyncPoolExecutor() as pool:
        for _ in range(20):
            pool.submit(asyncio.sleep(1))
    assert False


@pytest.mark.timeout(2)
async def test_map():
    async with async_pool.AsyncPoolExecutor() as pool:
        pool.map(asyncio.sleep, [1] * 10)


@pytest.mark.timeout(1)
async def test_shutdown():
    pool = async_pool.AsyncPoolExecutor()
    pool.map(asyncio.sleep, [5] * 1)
    await pool.shutdown(wait=False)


@pytest.mark.timeout(2)
async def test_wait():
    pool = async_pool.AsyncPoolExecutor()
    pool.map(asyncio.sleep, [1] * 15)
    await pool.wait()


@pytest.mark.timeout(2)
async def test_initializer():
    async with async_pool.AsyncPoolExecutor(initializer=print, initargs=('Initializing',)) as pool:
        pool.map(asyncio.sleep, [1] * 15)
