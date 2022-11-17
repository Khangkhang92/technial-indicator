from sqlalchemy import create_engine
from sqlalchemy.pool import NullPool


def get_engine():
    engine = create_engine(
        # "postgresql+psycopg2://postgres:1@localhost:1234/testdb3",
        "postgresql+psycopg2://postgres:myPassword@10.242.174.31:5432/webapp_db",
        pool_pre_ping=True,
        poolclass=NullPool,
        executemany_mode="values",
        executemany_values_page_size=10000,
        executemany_batch_page_size=500,
    )
    return engine
