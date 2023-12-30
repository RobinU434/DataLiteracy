from playwright.sync_api import Page
from pytest import fixture
from datetime import datetime
from os import makedirs

@fixture(scope='session', autouse=True)
def determine_current_date_and_create_folder():
    date = datetime.now().date().strftime(r"%d_%m_%Y")
    path = f"./scraped/{date}"
    makedirs(path, exist_ok=True)
    return (date, path)

def test_grap_weather_com(page: Page, determine_current_date_and_create_folder):
    (_, folder) = determine_current_date_and_create_folder
    page.goto("https://weather.com/weather/today/l/4ba49146d689a7603c2cf05be9ba81bbd618ff7194d50f77480551520a30952b")
    
    html_content = page.content()

    with open(f"{folder}/weather_com.html", "w", encoding="utf-8") as fp:
        fp.write(html_content)

def test_grap_weatheronline(page: Page, determine_current_date_and_create_folder):
    (_, folder) = determine_current_date_and_create_folder
    page.goto("https://www.wetteronline.de/wettertrend/tuebingen")

    page.frame_locator('#sp_message_iframe_925127').get_by_text("Akzeptieren & Weiter").click()

    html_content = page.content()

    with open(f"{folder}/weatheronline.html", "w", encoding="utf-8") as fp:
        fp.write(html_content)
