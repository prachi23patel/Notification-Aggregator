import os
import asyncio
import selectors

import pytest_asyncio
import redis.asyncio as redis

from dotenv import load_dotenv

from sqlalchemy.ext.asyncio import (
    create_async_engine,
    async_sessionmaker,
    AsyncSession,
)

from app.database import Base


# --------------------------------------------------
# Load .env from app/.env
# --------------------------------------------------

BASE_DIR = os.path.dirname(os.path.dirname(__file__))
ENV_PATH = os.path.join(BASE_DIR, "app", ".env")

load_dotenv(ENV_PATH)


# --------------------------------------------------
# Use SelectorEventLoop for psycopg on Windows
# --------------------------------------------------

def selector_event_loop():
    return asyncio.SelectorEventLoop(
        selectors.SelectSelector()
    )


def pytest_asyncio_loop_factories(config, item):
    return {
        "selector": selector_event_loop,
    }


# --------------------------------------------------
# Test configuration
# --------------------------------------------------

TEST_REDIS_URL = os.getenv("REDIS_TEST_URL")
TEST_DATABASE_URL = os.getenv("TEST_DATABASE_URL")


# --------------------------------------------------
# Redis fixture
# --------------------------------------------------

@pytest_asyncio.fixture
async def redis_client():

    client = redis.from_url(
        TEST_REDIS_URL,
        decode_responses=True
    )

    # Clean before test
    await client.flushdb()

    yield client

    # Clean after test
    await client.flushdb()

    await client.aclose()


# --------------------------------------------------
# PostgreSQL fixture
# --------------------------------------------------

@pytest_asyncio.fixture
async def db_session():

    # 1. Create engine for test database
    engine = create_async_engine(
        TEST_DATABASE_URL,
        echo=False
    )

    # 2. Create tables
    async with engine.begin() as conn:
        await conn.run_sync(
            Base.metadata.create_all
        )

    # 3. Create session factory
    SessionLocal = async_sessionmaker(
        engine,
        class_=AsyncSession,
        expire_on_commit=False
    )

    # 4. Give session to test
    async with SessionLocal() as session:
        yield session

    # 5. Drop tables after test
    async with engine.begin() as conn:
        await conn.run_sync(
            Base.metadata.drop_all
        )

    # 6. Close engine
    await engine.dispose()

@pytest_asyncio.fixture
async def session_factory():
    engine = create_async_engine(
        TEST_DATABASE_URL,
        echo=False
    )

    async with engine.begin() as conn:
        await conn.run_sync(
            Base.metadata.create_all
        )

    SessionLocal = async_sessionmaker(
        engine,
        class_=AsyncSession,
        expire_on_commit=False
    )

    yield SessionLocal

    async with engine.begin() as conn:
        await conn.run_sync(
            Base.metadata.drop_all
        )

    await engine.dispose()