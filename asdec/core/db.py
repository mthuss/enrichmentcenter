from asdec.core.config import Settings
from sqlalchemy.orm import DeclarativeBase
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine
# from core.config import Settings

class Base(DeclarativeBase):
    pass

class Database():
    def __init__(self, settings: Settings) -> None:
        # define parametros do engine aqui
        self.engine = create_async_engine( # colocar infos do engine escolhido aqui
            f"{settings.DB_DRIVER}://{settings.DB_USER}:{settings.DB_PASSWORD}@{settings.DB_HOST}:{settings.DB_PORT}/{settings.DB_NAME}",
            echo=False,
            hide_parameters=True,
            pool_size=16, 
            max_overflow=32
    )


        # realmente inicia a sessão
        self.sessionMaker = async_sessionmaker(
            self.engine,
            class_=AsyncSession,
            expire_on_commit=False
        )

    def getSession(self) -> AsyncSession:
        return self.sessionMaker()

    # similar ao "migrate" do django
    async def createTables(self) -> None:
        async with self.engine.begin() as conn:
            await conn.run_sync(Base.metadata.create_all) # pega todos os filhos da classe Base e cria tabelas no banco a partir dos metadados


# create feeds in the database
#def createFeeds():
    