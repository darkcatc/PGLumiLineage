"""
测试用的配置
"""
from pydantic import SecretStr

class DBSettings:
    USER = "lumiadmin"
    PASSWORD = SecretStr("lumiadmin")
    HOST = "localhost"
    PORT = 5432
    DB_NAME = "iwdb"
    DB_RAW_LOGS = "iwdb"
    DB_ANALYTICAL_PATTERNS = "iwdb"
    DB_AGE = "iwdb"

class Settings:
    INTERNAL_DB = DBSettings()

def get_settings_instance():
    return Settings()
