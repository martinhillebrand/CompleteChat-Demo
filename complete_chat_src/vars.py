import os

def DB_set_HOST(host: str):
    """Set TD_HOST environment variable."""
    os.environ["TD_HOST"] = host


def DB_set_USER(user: str):
    """Set TD_USER environment variable."""
    os.environ["TD_USER"] = user


def DB_set_DBC_PW(password: str):
    """Set TD_DBC_PASSWORD environment variable."""
    os.environ["TD_DBC_PASSWORD"] = password


def DB_set_USER_PW(password: str):
    """Set TD_USER_PASSWORD environment variable."""
    os.environ["TD_USER_PASSWORD"] = password


def set_OpenAI_key(api_key: str):
    """Set OPENAI_API_KEY environment variable."""
    os.environ["OPENAI_API_KEY"] = api_key
