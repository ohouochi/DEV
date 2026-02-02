import os

# Superset specific configuration
ROW_LIMIT = 5000
SUPERSET_WEBSERVER_PORT = 8088
SECRET_KEY = "QlJlkjApRxlCkGgipOnbQtVzYc2Zvnkkec1eHyj61feLMTL2QkhWUbSI"

SQLALCHEMY_DATABASE_URI = os.environ.get(
    "SUPERSET_SQLALCHEMY_DATABASE_URI",
    "postgresql://superset:superset@postgres:5432/superset"
)
SQLALCHEMY_TRACK_MODIFICATIONS = False

REDIS_HOST = os.environ.get("REDIS_HOST", "redis")
REDIS_PORT = int(os.environ.get("REDIS_PORT", 6379))
REDIS_DB = int(os.environ.get("REDIS_DB", 0))
REDIS_URL = os.environ.get("REDIS_URL", f"redis://{REDIS_HOST}:{REDIS_PORT}/{REDIS_DB}")

CACHE_CONFIG = {
    "CACHE_TYPE": "RedisCache",
    "CACHE_REDIS_URL": REDIS_URL,
    "CACHE_DEFAULT_TIMEOUT": 300,
}

SESSION_COOKIE_SECURE = False
SESSION_COOKIE_HTTPONLY = True
SESSION_COOKIE_SAMESITE = "Lax"
WTF_CSRF_ENABLED = True

SQL_MAX_ROW = 10000
SQLLAB_TIMEOUT = 30
DEFAULT_SQLLAB_LIMIT = 1000
CHART_CACHE_TIMEOUT = 300

CELERY_CONFIG = {
    "broker_url": REDIS_URL,
    "celery_result_backend": REDIS_URL,
    "celery_task_eager_propagates": True,
}

FEATURE_FLAGS = {
    "ALERT_REPORTS": True,
    "ENABLE_TEMPLATE_PROCESSING": True,
    "ENABLE_ROW_LEVEL_SECURITY": True,
}
