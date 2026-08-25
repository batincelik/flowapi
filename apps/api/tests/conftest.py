import os

os.environ.setdefault("FLOWAPI_SECRET_KEY", "test-only-secret-key-32-characters-long")
os.environ.setdefault("FLOWAPI_ENCRYPTION_KEY", "AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA=")
os.environ.setdefault("DATABASE_URL", "postgresql+asyncpg://flowapi:flowapi@localhost:5432/flowapi_test")
