from functools import lru_cache
from typing import Optional

from dotenv import load_dotenv, find_dotenv
from pydantic import BaseSettings, Field

load_dotenv(find_dotenv('local.env'))
# load_dotenv(find_dotenv('.env'))


class DbSettings(BaseSettings):
    db_host: Optional[str] = Field('', env='dbHost')
    db_port: Optional[int] = Field(3306, env='dbPort')
    db_name: Optional[str] = Field('', env='dbName')
    db_user: Optional[str] = Field('', env='dbUser')
    db_password: Optional[str] = Field('', env='dbPassword')
    db_engine: Optional[str] = 'mysql'

    class Config:
        env_file = ".env"
        env_file_encoding = 'utf-8'


class KeysAndConstants(BaseSettings):
    mysql_connector_jar: Optional[str] = Field('', env='mysql_connector_jar')
    token_aqi: Optional[str] = Field('', env='token_aqi')
    token_exchange_rate: Optional[str] = Field('', env='token_exchange_rate')
    dw_real_estate_url: Optional[str] = Field('', env='dw_real_estate_url')
    mysql_driver: Optional[str] = Field('', env='mysql_driver')
    real_estate_db_url: Optional[str] = Field('', env='real_estate_db_url')
    upload_bucket_name: Optional['str'] = Field('', env='upload_bucket_name')
    pyarrow_ignore_timezone: Optional[str] = Field('1', env='PYARROW_IGNORE_TIMEZONE')

    class Config:
        env_file = ".env"
        env_file_encoding = 'utf-8'


@lru_cache(maxsize=128)
def get_db_settings(file: str = 'local') -> DbSettings:
    db_settings = DbSettings(_env_file=f'{file}.env')
    return db_settings


@lru_cache(maxsize=128)
def get_keys_and_constants(file: str = 'local') -> KeysAndConstants:
    key_settings = KeysAndConstants(_env_file=f'{file}.env')
    return key_settings
