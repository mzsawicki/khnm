from pydantic_settings import BaseSettings


class KhnmSettings(BaseSettings):
    RABBITMQ_CONNECTION_STRING: str