from sqlalchemy import create_engine
from sqlalchemy.engine import Engine


def setup_engine(
    user_name: str,
    passwort: str,
    db_name: str,
    host_ip: str = "127.0.0.1",
    port: int = 3306,
) -> Engine:
    uri = (
        f"mariadb+mariadbconnector://{user_name}:{passwort}@{host_ip}:{port}/{db_name}"
    )
    engine = create_engine(uri)

    return engine
