from playwright.sync_api import Page
from pytest import fixture
from datetime import datetime
from os import makedirs#, cpu_count
import polars as pl
from time import sleep
# import concurrent.futures

@fixture(scope='session', autouse=True)
def determine_current_date_and_create_folder():
    date = datetime.now().date().strftime(r"%d_%m_%Y")
    path = f"./scraped/{date}"
    makedirs(path, exist_ok=True)
    return (date, path)

@fixture(scope='session', autouse=True)
def load_config_and_get_urls():
    config = pl.scan_csv("./config.csv")
    config: pl.LazyFrame = config.with_columns(
        pl.col("w_com_tenday_url").str.replace(r"\/tenday\/", r"/today/").alias("w_com_today_url")
    )
    config = config.collect()
    # config_head: pl.DataFrame = config.head().collect()
    # config_head.write_csv("test.csv")
    return config

def test_grab_weather_com(page: Page, determine_current_date_and_create_folder, load_config_and_get_urls):
    (_, folder) = determine_current_date_and_create_folder
    dwd_id = load_config_and_get_urls["dwd_station_id"]
    urls_today = load_config_and_get_urls["w_com_today_url"]
    urls_tenday = load_config_and_get_urls["w_com_tenday_url"]

    today_folder = f"{folder}/w_com/today"
    tenday_folder = f"{folder}/w_com/tenday"
    for s_folder in [tenday_folder, today_folder]:
        makedirs(s_folder, exist_ok=True)

    page.goto("https://weather.com/weather/today/l/4ba49146d689a7603c2cf05be9ba81bbd618ff7194d50f77480551520a30952b")
    
    html_content = page.content()

    with open(f"{folder}/weather_com.html", "w", encoding="utf-8") as fp:
        fp.write(html_content)

def test_grab_weatheronline(page: Page, determine_current_date_and_create_folder, load_config_and_get_urls):
    (_, folder) = determine_current_date_and_create_folder
    dwd_id = load_config_and_get_urls["dwd_station_id"]
    urls = load_config_and_get_urls["wo_de_url"]

    paren_folder = f"{folder}/wo/"
    makedirs(paren_folder, exist_ok=True)

    # i would have to switch to the async API to make this work in parallel... lets see if this works okay first...
    # with concurrent.futures.ThreadPoolExecutor() as exec:
    for (dwd_id, url) in zip(dwd_id, urls):
        print(url)

        page.goto(url, timeout=50000)

        page.frame_locator('#sp_message_iframe_925127').get_by_text("Akzeptieren & Weiter").click(timeout=50000)

        html_content = page.content()

        with open(f"{paren_folder}/{dwd_id}.html", "w", encoding="utf-8") as fp:
            fp.write(html_content)
        
        sleep(2)
