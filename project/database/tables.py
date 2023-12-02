from typing import List

from sqlalchemy import MetaData, Table, Column, Integer, String, Float, DateTime


class Database:
    def __init__(self) -> None:
        self._tables: List[Table]

    @staticmethod
    def _create_tables() -> List[Table]:
        meta = MetaData()

        sources = Table(
            "sources",
            meta,
            Column("id", Integer, primary_key=True),
            Column("name", String),  # Station name or location
            Column("latitude", Float),
            Column("longitude", Float),
            Column("provider", String),
            Column("height", Float)
        )

        api_calls = Table(
            "api_calls",
            meta,
            Column("id", Integer, primary_key=True),
            Column("source_id", Integer),
            Column("time", DateTime),  # time of call
        )

        weather_data = Table(
            "weather",
            meta,
            Column("id", Integer, primary_key=True),
            Column("api_call_id", Integer),
            Column("time", DateTime),  # time of weather
            Column("temperature", Float),
            Column("wind_speed", Float),
            Column("wind_direction", Float),
            Column("pressure", Float),
            Column("humidity", Float),
            Column("precipitation_total", Float),
            Column("precipitation_probablity", Float),
            Column("cloud_cover", Float)
        )
