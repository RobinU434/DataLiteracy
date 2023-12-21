import polars as pl
import geopy.distance as gpd
import datetime as dt
import json

def parse_and_filter_invalid_dates(data: pl.LazyFrame):
    for datecol in ["Beginn", "Ende"]:
        data = data.filter(pl.col(datecol) != " ")

        data = data.with_columns(
            pl.col(datecol).str.to_date(format = "%d.%m.%Y")
        )
    return data

def better_columns(data: pl.LazyFrame):
    return data.with_columns(
        pl.col("Kennung").cast(pl.Categorical()),
        pl.struct([pl.col("Breite").alias("latitude"), pl.col("Länge").alias("longitude")]).alias("coord"),
    )

def add_distance_tüb(data: pl.LazyFrame):
    tübingen_coord = data.filter(pl.col("Stationsname") == "Tübingen").first().collect()["coord"][0]

    return data.with_columns(
        (pl.col("coord")
            .map_elements(lambda coord: gpd.distance((coord['latitude'], coord['longitude']), (tübingen_coord['latitude'], tübingen_coord['longitude'])).kilometers, return_dtype=pl.Float32)
            .alias("distance_tübingen_km")),
    )

def filter_acceptable_stations(data: pl.LazyFrame):
    data = data.filter(pl.col("Beginn") < (dt.date.today() - dt.timedelta(days=1 * 12 * 30)))
    data = data.filter(pl.col("Ende") > (dt.date.today() - dt.timedelta(weeks=8)))
    data = data.filter(pl.col("distance_tübingen_km") < 20)
    return data

def groupby_location(data: pl.LazyFrame):
    return data.group_by(pl.col("coord")).agg([
        pl.col("distance_tübingen_km").first(),
        pl.struct(["Kennung", "Stations-kennung", "Beginn", "Ende", "Stationsname", "Stations_ID"]).alias("data_avail"),
        *[pl.col(colName).unique() for colName in  ["Stationsname", "Stations_ID"]],
    ])

def convert_to_obj(data: pl.LazyFrame):
    unformatted_json_str = data.collect().write_json(
        row_oriented=True,
        pretty=True, # doesnt work...
    )
    return json.loads(unformatted_json_str)

def dump_as_json_to(obj: object, path: str):
    with open(path, mode='wt') as fp:
        json.dump(
            obj, fp, 
            indent='\t', quote_keys=True, trailing_commas=False,
            ensure_ascii=False,
        )

def main():
    data = pl.scan_csv("./project/data/dwd/stations.tsv", separator="\t")
    data = add_distance_tüb(
        better_columns(
            parse_and_filter_invalid_dates(data)
        )
    )
    data = filter_acceptable_stations(data)
    data = groupby_location(data)
    data = data.sort(pl.col("distance_tübingen_km"))

    dump_as_json_to(convert_to_obj(data), path="./project/data/dwd/aggregated_station_info.json")

if __name__== "__main__":
    main()
