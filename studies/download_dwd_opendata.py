import json5
import zipfile
import requests
import os
import re
from io import BytesIO

def download_and_extract_zip(url: str, destination_folder: str, unique_descriptor: str | None):
    # Create the destination folder if it doesn't exist
    os.makedirs(destination_folder, exist_ok=True)

    # Download the zip file
    response = requests.get(url)
    descriptor = unique_descriptor or url
    if response.status_code == 200:
        # Extract the zip file content
        with zipfile.ZipFile(BytesIO(response.content)) as z:
            # Extract only files that match the specified pattern
            # for file_info in z.infolist():
            #     # if not file_pattern or re.match(file_pattern, file_info.filename):
            #     #     z.extract(file_info, destination_folder)
            #     #     print(f"Extracted: {file_info.filename} from {url}")
            unique_descriptor_stripped = re.sub(r'\s', '_', unique_descriptor)
            z.extractall(f"{destination_folder}/{unique_descriptor_stripped}")
        print(f"Downloaded and extracted: {descriptor}")
    else:
        print(f"Failed to download: {descriptor}")

with open("project/data/dwd/aggregated_station_info.json") as fp:
    ret = json5.load(fp)

for position in ret:
    accepted_measurement = "MN"
    accepted_stations = [measurement for measurement in position["data_avail"] if "MN" == measurement["Kennung"]]

    if len(accepted_stations) == 0:
        print(f"Skipped because theres no accepted measurement: {position['Stationsname']}")
    for station in accepted_stations:
        download_and_extract_zip(
            f"https://opendata.dwd.de/climate_environment/CDC/observations_germany/climate/hourly/precipitation/recent/stundenwerte_RR_0{station['Stations_ID']}_akt.zip",
            destination_folder="data/dwd/actual_observations/raw",
            unique_descriptor=station["Stationsname"],
        )

