from pydantic_settings import BaseSettings

class Settings(BaseSettings):
    DB_USER: str = "asdec_user"
    DB_NAME: str = "asdec-debug"
    DB_PORT: str = "5432"
    DB_HOST: str = "localhost"
    DB_PASSWORD: str = "e06d8bfca5174428a23b0d3916199ca6"
    DB_DRIVER: str = "postgresql+asyncpg"
    REDIS_USER: str = "acme"
    REDIS_PASSWORD: str = "nevermore"
    MISP_API_KEY: str = "MISP_API_KEY"
    BLOCKLIST_CSV_PATH: str = "asdec/data/blocklists.csv"
    WHITELIST_CSV_PATH: str = "asdec/data/whitelists.csv"

    class Config:
        env_file = "../.env"
