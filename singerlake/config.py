from pydantic import BaseModel


class SingerlakeConfig(BaseModel):
    """Singer Lake Config."""

    store: dict = {}
    lock: dict = {}
    paths: dict = {}
