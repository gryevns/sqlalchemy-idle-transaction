import asyncio
import contextlib

from sqlalchemy import Column
from sqlalchemy import DateTime
from sqlalchemy import ForeignKey
from sqlalchemy import func
from sqlalchemy import Integer
from sqlalchemy import String
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.ext.asyncio import create_async_engine
from sqlalchemy.future import select
from sqlalchemy.orm import declarative_base
from sqlalchemy.orm import relationship
from sqlalchemy.orm import selectinload
from sqlalchemy.orm import sessionmaker

class _Base:
    __mapper_args__ = {"eager_defaults": True}

Base = declarative_base(cls=_Base)

class Parent(Base):
    __tablename__ = "a"
    id = Column(Integer, primary_key=True)
    data = Column(String())
    children = relationship("Child")

class Child(Base):
    __tablename__ = "b"
    id = Column(Integer, primary_key=True)
    parent_id = Column(ForeignKey("a.id"))

class UnitOfWorkFactory():
    def __init__(self, session_factory: sessionmaker):
        self._session_factory = session_factory

    @contextlib.asynccontextmanager
    async def __call__(self):
        uow = self._session_factory()
        try:
            yield uow
        finally:
            await uow.rollback()
            await uow.close()

async def async_main():
    engine = create_async_engine(
        "postgresql+asyncpg://postgres:password@127.0.0.1:5432",
        echo=True,
        future=True,
        pool_size=10,
        max_overflow=5,
        pool_timeout=5,
        pool_pre_ping=True,
    )

    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.drop_all)
        await conn.run_sync(Base.metadata.create_all)

    session_factory = sessionmaker(engine, class_=AsyncSession, future=True)
    uow_factory = UnitOfWorkFactory(session_factory)

    async with uow_factory() as uow:
        uow.add(Parent(children=[Child()]))
        await uow.commit()

    try:
        async with uow_factory() as uow:
            stmt = select(Parent).with_for_update(of=Parent)
            result = await uow.execute(stmt)
            for item in result.scalars():
                item.data = "updated"
                item.children  # Raises sqlalchemy.exc.MissingGreenlet
    except:
        await asyncio.sleep(30)
        raise

asyncio.run(async_main())
