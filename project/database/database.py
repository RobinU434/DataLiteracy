from typing import List
from pandas import DataFrame
from project.database.utils.decorator import check_connection

from sqlalchemy import Date, MetaData, Table, Column, Integer, String, Float, DateTime, ForeignKey
from sqlalchemy.engine import Engine

from project.database.utils.db_connection import setup_engine


class Database:
    def __init__(
        self,
        user_name: str,
        password: str,
        db_name: str,
        host_ip: str = "127.0.0.1",
        port: int = 3306,
    ) -> None:
        # connection credential
        self._user_name = user_name
        self._password = password
        self._db_name = db_name
        self._host_ip = host_ip
        self._port = port

        self._sources: Table
        self._api_calls: Table
        self._temperature: Table
        self._wind_data: Table
        self._pressure: Table
        self._humidity: Table
        self._precipitation: Table
        self._cloud: Table
        self._daily: Table
        self._meta = self._setup_meta_data()

        self._engine: Engine

    def connect(self):
        if hasattr(self, "_engine"):
            return

        self._engine = setup_engine(
            user_name=self._user_name,
            passwort=self._passwort,
            db_name=self._db_name,
            host_ip=self._host_ip,
            port=self._port,
        )


    def _setup_meta_data(self) -> MetaData:
        meta = MetaData()

        self._sources = Table(
            "sources",
            meta,
            Column("id", Integer, primary_key=True),
            Column("name", String(64)),  # Station name or location
            Column("latitude", Float),
            Column("longitude", Float),
            Column("provider", String(64)),  # special case: if one provider has multiple forecasts -> _XX at the end
            Column("height", Float),
        )

        self._api_calls = Table(
            "api_calls",
            meta,
            Column("id", Integer, primary_key=True),
            Column("source_id", ForeignKey("sources.id")),
            Column("time", DateTime),  # time of call
        )

        self._temperature = Table(
            "temperature",
            meta,
            Column("id", Integer, primary_key=True),
            Column("api_call_id", ForeignKey("api_calls.id")),
            Column("time", DateTime),  # time of weather
            Column("temperature", Float),
            Column("temperature_std", Float),
        )

        self._wind_data = Table(
            "wind",
            meta,
            Column("id", Integer, primary_key=True),
            Column("api_call_id", ForeignKey("api_calls.id")),
            Column("time", DateTime),  # time of weather
            Column("wind_speed", Float),
            Column("wind_direction", Float),
        )

        self._pressure = Table(
            "pressure",
            meta,
            Column("id", Integer, primary_key=True),
            Column("api_call_id", ForeignKey("api_calls.id")),
            Column("time", DateTime),  # time of weather
            Column("pressure", Float),
        )

        self._humidity =  Table(
            "humidity",
            meta,
            Column("id", Integer, primary_key=True),
            Column("api_call_id", ForeignKey("api_calls.id")),
            Column("time", DateTime),  # time of weather
            Column("humidity", Float),
        )

        self._precipitation = Table(
            "precipitation",
            meta,
            Column("id", Integer, primary_key=True),
            Column("api_call_id", ForeignKey("api_calls.id")),
            Column("time", DateTime),  # time of weather
            Column("precipitation_total", Float),
            Column("precipitation_probablity", Float),
        )

        self._cloud = Table(
            "cloud",
            meta,
            Column("id", Integer, primary_key=True),
            Column("api_call_id", ForeignKey("api_calls.id")),
            Column("time", DateTime),  # time of weather
            Column("cloud_cover", Float),
        )

        self._daily = Table(
            "daily_data",
            meta,
            Column("id", Integer, primary_key=True),
            Column("api_call_id", ForeignKey("api_calls.id")),
            Column("day", Date),
            Column("temperatur_min", Float),
            Column("temperatur_max", Float),
            Column("precipetation", Float),
            Column("wind_speed", Float),
            Column("wind_direction", Float)
        )
        self._daily.insert()

        return meta

    @check_connection
    def build_tables(self):
        self._meta.create_all(self._engine)

    @check_connection
    def insert(self):
        pass