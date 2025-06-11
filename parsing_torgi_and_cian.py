import itertools
import os
import signal
import time
from dataclasses import dataclass, field
from datetime import datetime
from functools import cached_property
from pathlib import Path
from typing import Callable
from uuid import uuid4, UUID

import schedule
from fake_useragent import UserAgent
from google.auth.exceptions import RefreshError
from google.auth.transport.requests import Request
from google.oauth2.credentials import Credentials
from google_auth_oauthlib.flow import InstalledAppFlow

# noinspection PyProtectedMember
from googleapiclient.discovery import build, Resource

from dateutil import tz
import orjson
import requests
from selenium.common import NoSuchElementException
from selenium.webdriver.common.by import By
from tqdm import tqdm
from bs4 import BeautifulSoup
from undetected_chromedriver import ChromeOptions, Chrome

REPEAT_DELAY_HOURS = 0  # 0 means no repeat

GOOGLE_SCOPES = ["https://www.googleapis.com/auth/spreadsheets"]
GOOGLE_SPREADSHEET_ID = "1wir2oXE8XG4K3krRsw95OIJAnvFUeAZQRKuPy9BWKOM"

CHROME_EXECUTABLE_PATH = Path.home() / ".local/share/chrome-linux64/chrome"

GOOGLE_CREDENTIALS_PATH = Path("google-auth/credentials-spreadsheets.json")
GOOGLE_TOKEN_PATH = Path("google-auth/token-spreadsheets.json")

MOSCOW_TZ = tz.gettz("Europe/Moscow")
DATETIME_FORMAT_STRING = "%d.%m.%Y %H:%M:%S"
TIME_FORMAT_STRING = "%H:%M:%S"
FALLBACK_DATETIME = datetime(year=1970, month=1, day=1)

NO_DATA_MESSAGE = "нет данных"
IGNORE_PROPERTY_CATEGORIES = [
    "Гаражи и машиноместа",
    "Машиноместо",
]

driver: Chrome
service: Resource
refresh_main_page: Callable[[], None]

federal_law_hints = {
    hint["code"]: hint["fullName"]
    for hint in requests.get(
        "https://torgi.gov.ru/new/nsi/v1/RELATIONSHIP_BIDD_HINTEXT"
    ).json()
}
moscow_district_name_to_cian_id: dict[str, int] | None = None

address_replacements = {
    "р-н": "район",
    "пр-кт": "проспект",
}


class URLs:
    CIAN_MAIN_URL = "https://cian.ru/"
    CIAN_DISTRICTS = "https://www.cian.ru/api/geo/get-districts-tree/?locationId=1"
    CIAN_GEOCODE = "https://www.cian.ru/api/geo/geocode-cached/?request={}"
    CIAN_GEOCODE_FOR_SEARCH = "https://www.cian.ru/api/geo/geocoded-for-search/"
    CIAN_SALE_SEARCH = (
        "https://www.cian.ru/cat.php?deal_type=sale&engine_version=2"
        "&offer_type=offices&office_type[0]=1&office_type[1]=2&office_type[2]=3&office_type[3]=4&office_type[4]=5"
        "&office_type[5]=7&office_type[6]=11&{}"
    )
    CIAN_SALE_SEARCH_LAND = (
        "https://www.cian.ru/cat.php?cats[0]=commercialLandSale&deal_type=sale&engine_version=2"
        "&offer_type=offices&{}"
    )
    CIAN_RENT_SEARCH = (
        "https://www.cian.ru/cat.php?deal_type=rent&engine_version=2"
        "&offer_type=offices&office_type[0]=1&office_type[1]=2&office_type[2]=3&office_type[3]=4&office_type[4]=5"
        "&office_type[5]=7&office_type[6]=11&{}"
    )
    CIAN_RENT_SEARCH_LAND = (
        "https://www.cian.ru/cat.php?cats[0]=commercialLandRent&deal_type=rent&engine_version=2"
        "&offer_type=offices&{}"
    )
    TORGI_SEARCH_API = (
        "https://torgi.gov.ru/new/api/public/lotcards/search"
        "?dynSubjRF=78,53"
        "&biddType=178FZ,1041PP,229FZ,701PP,ZKPT,KGU"
        "&catCode=7"
        "&lotStatus=PUBLISHED,APPLICATIONS_SUBMISSION"
        "&ownForm=12,13,14"
        "&withFacets=false"
        "&page={}"
        "&sort=firstVersionPublicationDate,desc"
    )
    TORGI_LOT_API = "https://torgi.gov.ru/new/api/public/lotcards/{}"
    TORGI_LOG_PAGE = "https://torgi.gov.ru/new/public/lots/lot/{}"


@dataclass(frozen=True)
class TorgiLot:
    name: str
    address: str
    area: float
    price: float
    url: str
    notice_number: str
    lot_number: int
    auction_type: str
    sale_type: str
    law_reference: str
    application_start: datetime
    application_end: datetime
    auction_start: datetime
    cadastral_number: str
    property_category: str
    ownership_type: str
    auction_step: float
    deposit: float
    recipient: str
    recipient_inn: str
    recipient_kpp: str
    bank_name: str
    bank_bic: str
    bank_account: str
    correspondent_account: str
    auction_url: str
    uuid: UUID = field(default_factory=lambda: uuid4())
    # uuid is used only for linking lots and offers

    @cached_property
    def custom_category(self) -> str:
        if "офис" in self.name.lower() and 1000 <= self.area <= 3500:
            return "Офис (от 1000 до 3500 м²)"

        if self.property_category in ["Сооружения", "Здания"]:
            return "Отдельно стоящее здание"

        if self.property_category in [
            "Нежилые помещения",
            "Квартиры",
            "Машиноместо",
            "Гаражи и машиноместа",
        ]:
            if self.area <= 1000:
                area_string = "до 1000 м²"
            elif self.area <= 3000:
                area_string = "от 1000 до 3000 м²"
            else:
                area_string = "от 3000 м²"
            return f"Промышленное помещение {area_string}"

        if self.property_category.startswith("Зем") and self.area >= 10000:
            return "Коммерческая земля от 1 га"

        if self.area <= 100:
            area_string = "до 100 м²"
        elif self.area <= 250:
            area_string = "от 100 до 250 м²"
        elif self.area <= 500:
            area_string = "от 250 до 500 м²"
        elif self.area <= 1000:
            area_string = "от 500 до 1000 м²"
        elif self.area <= 1500:
            area_string = "от 1000 до 1500 м²"
        else:
            area_string = "от 1500 м²"

        return f"Стрит ритейл {area_string}"


@dataclass(frozen=True)
class CianSaleOffer:
    address: str
    area: float
    price: float
    url: str
    lot_uuid: UUID


@dataclass(frozen=True)
class CianRentOffer:
    address: str
    area: float
    price: float
    url: str
    lot_uuid: UUID


def info(message: str) -> None:
    current_moment = datetime.now()
    tqdm.write(f"{current_moment.strftime(TIME_FORMAT_STRING)} INFO: {message}")


def info_with_followup(message: str) -> None:
    current_moment = datetime.now()
    tqdm.write(f"{current_moment.strftime(TIME_FORMAT_STRING)} INFO: {message}", end="\t")


def info_followup(message: str) -> None:
    tqdm.write(message)


def driver_get_page(url: str) -> str:
    try:
        driver.get(url)
        return driver.page_source
    except Exception:
        driver_setup()
        return driver_get_page(url)


def driver_get_json(url: str, _retries: int = 0) -> ...:
    try:
        driver.get(url)
    except Exception:
        driver_setup()
        return driver_get_json(url)

    if _retries > 0:
        time.sleep(3)

    try:
        return orjson.loads(driver.find_element(By.TAG_NAME, "pre").text)
    except NoSuchElementException:
        if _retries < 10:
            time.sleep(1)
            return driver_get_json(url, _retries + 1)

        raise


def driver_post(url: str, body: dict[str, ...], _retries: int = 0) -> ...:
    try:
        driver.execute_script(
            """
            function post(path, params, method='post') {
                const form = document.createElement('form');
                form.method = method;
                form.action = path;

                for (const key in params) {
                    if (params.hasOwnProperty(key)) {
                        const hiddenField = document.createElement('input');
                        hiddenField.type = 'hidden';
                        hiddenField.name = key;
                        hiddenField.value = params[key];

                        form.appendChild(hiddenField);
                    }
                }

                document.body.appendChild(form);
                form.submit();
            }

            post(arguments[1], arguments[0]);
            """,
            body,
            url,
        )
    except Exception:
        driver_setup()
        driver_post(url, body)

    if _retries > 0:
        time.sleep(3)

    try:
        return orjson.loads(driver.find_element(By.TAG_NAME, "pre").text)
    except NoSuchElementException:
        if _retries < 10:
            time.sleep(1)
            return driver_get_json(url, _retries + 1)

        raise


def get_lot_table_row(
    lot: TorgiLot, row_num: int, price_history: str = None
) -> list[...]:
    current_history_record = f"{int(lot.price)},"

    price_history = price_history or current_history_record
    if not price_history.startswith(current_history_record):
        price_history = f"{current_history_record}\n{price_history}"

    return [
        row_num - 1,  # always 1 less than row_num because of the title row
        lot.name,
        lot.address,
        lot.custom_category,
        lot.area,
        f"=I{row_num}/E{row_num}",
        f'=IFERROR(J{row_num}/E{row_num}; "{NO_DATA_MESSAGE}")',
        price_history,
        lot.price,
        f'=IFERROR(MEDIAN(ARRAYFORMULA(IF(cian_sale!G:G=AK{row_num}; cian_sale!E:E))); "{NO_DATA_MESSAGE}")',
        f'=IFERROR(J{row_num}-I{row_num}; "{NO_DATA_MESSAGE}")',
        f'=IFERROR((J{row_num}/I{row_num})-100%; "{NO_DATA_MESSAGE}")',
        f'=IFERROR(MEDIAN(ARRAYFORMULA(IF(cian_rent!G:G=AK{row_num}; cian_rent!E:E))); "{NO_DATA_MESSAGE}")',
        f'=IFERROR(M{row_num}/J{row_num}; "{NO_DATA_MESSAGE}")',
        lot.url,
        lot.notice_number,
        lot.lot_number,
        lot.auction_type,
        lot.sale_type,
        lot.law_reference,
        lot.application_start.strftime(DATETIME_FORMAT_STRING),
        lot.application_end.strftime(DATETIME_FORMAT_STRING),
        lot.auction_start.strftime(DATETIME_FORMAT_STRING),
        lot.cadastral_number,
        lot.property_category,
        lot.ownership_type,
        lot.auction_step,
        lot.deposit,
        lot.recipient,
        lot.recipient_inn,
        lot.recipient_kpp,
        lot.bank_name,
        lot.bank_bic,
        lot.bank_account,
        lot.correspondent_account,
        lot.auction_url,
        str(lot.uuid),
    ]


def get_offer_table_row(
    offer: CianSaleOffer | CianRentOffer, row_num: int
) -> list[...]:
    return [
        row_num - 1,  # always 1 less than row_num because of the title row
        offer.address,
        offer.area,
        f"=E{row_num}/C{row_num}",
        offer.price,
        offer.url,
        str(offer.lot_uuid),
    ]


def upload(
    *,
    lots: list[TorgiLot] | None = None,
    sale_offers: list[CianSaleOffer] | None = None,
    rent_offers: list[CianRentOffer] | None = None,
) -> None:
    try:
        for offer_list, sheet_name in [
            (sale_offers, "cian_sale"),
            (rent_offers, "cian_rent"),
        ]:
            if not offer_list:
                continue

            cian_num_column = (
                service.spreadsheets()
                .values()
                .get(
                    spreadsheetId=GOOGLE_SPREADSHEET_ID,
                    range=f"{sheet_name}!A:A",
                    majorDimension="COLUMNS",
                )
                .execute()
            )
            try:
                cian_last_row_num = int(
                    cian_num_column.get("values", [["title", 0]])[0][-1]
                )
            except ValueError:
                cian_last_row_num = 0

            # cian_url_column = (
            #     service.spreadsheets()
            #     .values()
            #     .get(
            #         spreadsheetId=GOOGLE_SPREADSHEET_ID,
            #         range=f"{sheet_name}!F:F",
            #         majorDimension="COLUMNS",
            #     )
            #     .execute()
            # )
            # present_offer_url_to_row_num = dict(
            #     zip(
            #         cian_url_column.get("values", [[]])[0][1:],
            #         itertools.count(2),
            #     )
            # )

            new_offers = [
                offer
                for offer in offer_list
                # if offer.url not in present_offer_url_to_row_num
            ]
            # present_offers_to_row_num = {
            #     offer: present_offer_url_to_row_num[offer.url]
            #     for offer in offer_list
            #     if offer.url in present_offer_url_to_row_num
            # }

            (
                service.spreadsheets()
                .values()
                .append(
                    spreadsheetId=GOOGLE_SPREADSHEET_ID,
                    range=sheet_name,
                    valueInputOption="USER_ENTERED",
                    body={
                        "values": [
                            get_offer_table_row(offer, row_num)
                            for row_num, offer in enumerate(
                                new_offers, start=cian_last_row_num + 2
                            )
                            # 2 = {+1 offset due to title} + {+1 because it is the next}
                        ]
                    },
                )
                .execute()
            )

            # (
            #     service.spreadsheets()
            #     .values()
            #     .batchUpdate(
            #         spreadsheetId=GOOGLE_SPREADSHEET_ID,
            #         body={
            #             "valueInputOption": "USER_ENTERED",
            #             "data": [
            #                 {
            #                     "range": f"{sheet_name}!{row_num}:{row_num}",
            #                     "values": [get_offer_table_row(offer, row_num)],
            #                 }
            #                 for offer, row_num in present_offers_to_row_num.items()
            #             ],
            #         },
            #     )
            # )

        if not lots:
            return

        torgi_num_column = (
            service.spreadsheets()
            .values()
            .get(
                spreadsheetId=GOOGLE_SPREADSHEET_ID,
                range="torgi!A:A",
                majorDimension="COLUMNS",
            )
            .execute()
        )
        try:
            torgi_last_row_num = int(torgi_num_column.get("values", [["title", 0]])[0][-1])
        except ValueError:
            torgi_last_row_num = 0

        torgi_url_column = (
            service.spreadsheets()
            .values()
            .get(
                spreadsheetId=GOOGLE_SPREADSHEET_ID,
                range="torgi!O:O",
                majorDimension="COLUMNS",
            )
            .execute()
        )
        present_lot_url_to_row_num: dict[str, int] = dict(
            zip(
                torgi_url_column.get("values", [[]])[0][1:],
                itertools.count(2),
            )
        )
        torgi_price_history_column = (
            service.spreadsheets()
            .values()
            .get(
                spreadsheetId=GOOGLE_SPREADSHEET_ID,
                range="torgi!H:H",
                majorDimension="COLUMNS",
            )
            .execute()
        )
        present_lot_url_to_price_history: dict[str, str] = dict(
            zip(
                torgi_url_column.get("values", [[]])[0][1:],
                torgi_price_history_column.get("values", [[]])[0][1:],
            )
        )

        new_lots = [lot for lot in lots if lot.url not in present_lot_url_to_row_num]
        present_lots_to_row_num_and_price_history = {
            lot: (
                present_lot_url_to_row_num[lot.url],
                present_lot_url_to_price_history[lot.url],
            )
            for lot in lots
            if lot.url in present_lot_url_to_row_num
        }

        (
            service.spreadsheets()
            .values()
            .append(
                spreadsheetId=GOOGLE_SPREADSHEET_ID,
                range="torgi",
                valueInputOption="USER_ENTERED",
                body={
                    "values": [
                        get_lot_table_row(lot, row_num)
                        for row_num, lot in enumerate(
                            new_lots, start=torgi_last_row_num + 2
                        )
                        # 2 = {+1 offset due to title} + {+1 because it is the next}
                    ]
                },
            )
            .execute()
        )

        (
            service.spreadsheets()
            .values()
            .batchUpdate(
                spreadsheetId=GOOGLE_SPREADSHEET_ID,
                body={
                    "valueInputOption": "USER_ENTERED",
                    "data": [
                        {
                            "range": f"torgi!{row_num}:{row_num}",
                            "values": [get_lot_table_row(lot, row_num, price_history)],
                        }
                        for lot, (
                            row_num,
                            price_history,
                        ) in present_lots_to_row_num_and_price_history.items()
                    ],
                },
            )
        )
    except Exception:
        upload(lots=lots, rent_offers=rent_offers, sale_offers=sale_offers)


def unformatted_address_to_cian_search_filter(address: str) -> str:
    # noinspection PyBroadException
    try:
        for old, new in address_replacements.items():
            address = address.replace(old, new)

        geocoding_response = driver_get_json(URLs.CIAN_GEOCODE.format(address))
        geocoding_result = next(
            item
            for item in geocoding_response["items"]
            if item["text"].startswith("Россия, Моск")  # ...ва, ...овская область
        )
        lon, lat = geocoding_result["coordinates"]

        if "Московская область" in geocoding_result["text"]:
            for_search_result = driver_post(
                URLs.CIAN_GEOCODE_FOR_SEARCH,
                {"lat": lat, "lng": lon, "kind": "locality"},
            )
            # details levels structure: 0 - МО, 1 - округ/город, 2 - дальше

            return f"location[0]={for_search_result['details'][1]['id']}"
        else:  # "Москва"
            for_search_result = driver_post(
                URLs.CIAN_GEOCODE_FOR_SEARCH,
                {"lat": lat, "lng": lon, "kind": "district"},
            )
            # details levels structure: 0 - Москва, 1 - АО, 2 - "район Ховрино", 3 - дальше

            try:
                district_name = (
                    for_search_result["details"][2]["fullName"]
                    .replace("район", "")
                    .replace("р-н", "")
                    .strip()
                )

                return f"district[0]={moscow_district_name_to_cian_id[district_name]}"
            except LookupError:
                def key_function(pair):
                    return index if (index := address.find(pair[0])) != -1 else len(address)

                found_district_name_and_id = min(
                    moscow_district_name_to_cian_id.items(),
                    key=key_function,
                )

                if key_function(found_district_name_and_id) < len(address):
                    return f"district[0]={found_district_name_and_id[1]}"

                for_search_result = driver_post(
                    URLs.CIAN_GEOCODE_FOR_SEARCH,
                    {"lat": lat, "lng": lon, "kind": "street"},
                )

                return f"street[0]={for_search_result['details'][-1]['id']}"
    except (LookupError, StopIteration):
        info(f"Unable to locate address '{address}'")
        return "region=4593" if "Московская область" in address else "region=1"


def extract_lot_characteristic(
    lot_info: dict[str, ...], characteristic_code: str, default: ...
) -> str:
    return next(
        filter(
            lambda characteristic: characteristic["code"] == characteristic_code,
            lot_info["characteristics"],
        ),
        {"characteristicValue": default},
    )["characteristicValue"]


def parse_torgi() -> list[TorgiLot]:
    bar = tqdm(desc="torgi pages")

    try:
        lots: list[TorgiLot] = []

        for page in itertools.count():
            search_response = requests.get(URLs.TORGI_SEARCH_API.format(page))
            search_response_json: dict[str, ...] = search_response.json()

            if search_response_json["empty"]:
                break

            bar.total = search_response_json["totalPages"]

            for lot_search_result in tqdm(
                search_response_json["content"], desc="lots on page", leave=False
            ):
                lot_id = lot_search_result["id"]

                lot_response = requests.get(URLs.TORGI_LOT_API.format(lot_id))
                lot_info: dict[str, ...] = lot_response.json()

                if lot_info["category"]["name"] in IGNORE_PROPERTY_CATEGORIES:
                    continue

                lots.append(
                    TorgiLot(
                        name=lot_info["lotName"],
                        address=lot_info["estateAddress"],
                        area=float(
                            extract_lot_characteristic(lot_info, "totalAreaRealty", 0)
                        ),
                        price=lot_info["priceMin"],
                        url=URLs.TORGI_LOG_PAGE.format(lot_id),
                        notice_number=lot_info["noticeNumber"],
                        lot_number=lot_info["lotNumber"],
                        auction_type=lot_info["biddForm"]["name"],
                        sale_type=lot_info["biddType"]["name"],
                        law_reference=federal_law_hints[lot_info["npaHintCode"]],
                        application_start=datetime.fromisoformat(
                            lot_info["biddStartTime"]
                        ).astimezone(MOSCOW_TZ),
                        application_end=datetime.fromisoformat(
                            lot_info["biddEndTime"]
                        ).astimezone(MOSCOW_TZ),
                        auction_start=datetime.fromisoformat(
                            lot_info.get("auctionStartDate", FALLBACK_DATETIME.isoformat())
                        ).astimezone(MOSCOW_TZ),
                        cadastral_number=extract_lot_characteristic(
                            lot_info, "cadastralNumberRealty", ""
                        ),
                        property_category=lot_info["category"]["name"],
                        ownership_type=lot_info["ownershipForm"]["name"],
                        auction_step=lot_info.get("priceStep", 0),
                        deposit=lot_info.get("deposit", 0),
                        recipient=lot_info["depositRecipientName"],
                        recipient_inn=lot_info["depositRecipientINN"],
                        recipient_kpp=lot_info["depositRecipientKPP"],
                        bank_name=lot_info["depositBankName"],
                        bank_bic=lot_info["depositBIK"],
                        bank_account=lot_info["depositPayAccount"],
                        correspondent_account=lot_info["depositCorAccount"],
                        auction_url=lot_info.get("etpUrl", ""),
                    )
                )

            bar.update()

        return lots
    except Exception:
        bar.close()
        info("Retrying to get lots...")
        return parse_torgi()


def parse_cian_nearby(lots: list[TorgiLot]) -> None:
    for lot in tqdm(lots, desc="lots on cian"):
        refresh_main_page()

        sale_offers = []
        rent_offers = []

        search_filter = unformatted_address_to_cian_search_filter(lot.address)

        for offer_list, offer_dataclass, url, tqdm_desc in [
            (sale_offers, CianSaleOffer, URLs.CIAN_SALE_SEARCH, "sale offers"),
            (
                sale_offers,
                CianSaleOffer,
                URLs.CIAN_SALE_SEARCH_LAND,
                "sale offers (land)",
            ),
            (rent_offers, CianRentOffer, URLs.CIAN_RENT_SEARCH, "rent offers"),
            (
                rent_offers,
                CianRentOffer,
                URLs.CIAN_RENT_SEARCH_LAND,
                "rent offers (land)",
            ),
        ]:
            search_url = url.format(search_filter)
            search_page = driver_get_page(search_url)
            search_soup = BeautifulSoup(search_page, features="lxml")
            offer_urls = tuple(
                map(
                    lambda anchor: anchor["href"],
                    search_soup.find_all("a", attrs={"data-name": "CommercialTitle"}),
                )
            )

            for offer_url in tqdm(offer_urls, desc=tqdm_desc, leave=False):
                offer_page = driver_get_page(offer_url)
                offer_soup = BeautifulSoup(offer_page, features="lxml")

                try:
                    script_tag = next(
                        tag
                        for tag in offer_soup.find_all("script")
                        if "window._cianConfig['frontend-offer-card']" in tag.text
                    )
                except StopIteration:
                    continue

                config_json_string = (
                    script_tag.text.strip().split(".concat(", 1)[1].rsplit(");", 1)[0]
                )
                config_json: list[dict] = orjson.loads(config_json_string)
                offer_info = next(
                    filter(lambda block: block["key"] == "defaultState", config_json)
                )["value"]

                area: float | None = None

                try:
                    if "land" in offer_info["offerData"]["offer"]:
                        match offer_info["offerData"]["offer"]["land"]["areaUnitType"]:
                            case "sotka":
                                area = float(offer_info["offerData"]["offer"]["land"]["area"]) * 100
                            case "hectare":
                                area = float(offer_info["offerData"]["offer"]["land"]["area"]) * 10000
                except (LookupError, ValueError):
                    pass

                if area is None:
                    area = float(offer_info["offerData"]["offer"].get("totalArea"))

                if area is None:
                    continue

                try:
                    offer_list.append(
                        offer_dataclass(
                            lot_uuid=lot.uuid,
                            address=offer_info["adfoxOffer"]["response"]["data"][
                                "unicomLinkParams"
                            ]["puid14"],
                            area=area,
                            price=offer_info["offerData"]["offer"].get(
                                "priceTotalRur",
                                offer_info["offerData"]["offer"].get(
                                    "priceTotalPerMonthRur",
                                    0,
                                ),
                            ),
                            url=offer_url,
                        )
                    )
                except Exception:
                    info(f"Something wrong with offer '{offer_url}'")
                    continue

        upload(lots=[lot], sale_offers=sale_offers, rent_offers=rent_offers)


def google_auth() -> Resource:
    creds = None

    if GOOGLE_TOKEN_PATH.exists():
        creds = Credentials.from_authorized_user_file(
            str(GOOGLE_TOKEN_PATH), GOOGLE_SCOPES
        )

    if creds and not creds.valid:
        try:
            creds.refresh(Request())
        except RefreshError:
            creds = None

    if not creds:
        flow = InstalledAppFlow.from_client_secrets_file(
            str(GOOGLE_CREDENTIALS_PATH), GOOGLE_SCOPES
        )
        creds = flow.run_local_server(port=0)

    GOOGLE_TOKEN_PATH.write_text(creds.to_json())

    return build("sheets", "v4", credentials=creds)


def driver_setup() -> None:
    global driver, refresh_main_page

    info_with_followup("setting up driver...")

    try:
        if driver is not None:
            driver.quit()
            os.kill(driver.browser_pid, signal.SIGKILL)  # chrome process (eats up to 3gb of RAM)
            os.kill(driver.browser_pid + 1, signal.SIGKILL)  # undetected_chromedriver process
    except NameError:
        pass

    try:
        os.putenv("GLOBAL_DEFAULT_TIMEOUT", "1200")

        options = ChromeOptions()
        options.add_argument(f"--user-agent={UserAgent(browsers=["Chrome"]).random}")
        options.add_argument("--disable-blink-features=AutomationControlled")
        options.page_load_strategy = "eager"

        driver = Chrome(
            options=options,
            headless=True,
            browser_executable_path=str(CHROME_EXECUTABLE_PATH)
        )
        driver.set_page_load_timeout(300)

        driver.get(URLs.CIAN_MAIN_URL)
        time.sleep(3)
        first_tab = driver.current_window_handle

        def refresh_declaration():
            nonlocal first_tab

            try:
                current_tab = driver.current_window_handle

                driver.switch_to.window(first_tab)
                driver.refresh()
                time.sleep(3)

                driver.switch_to.window(current_tab)
            except Exception:
                driver_setup()

        refresh_main_page = refresh_declaration

        driver.switch_to.new_window("tab")

        info_followup("driver set up")
    except Exception:
        time.sleep(60)
        driver_setup()


def setup() -> None:
    global service, moscow_district_name_to_cian_id

    service = google_auth()

    driver_setup()

    moscow_district_name_to_cian_id = dict(
        (district["name"], district["id"])
        if adm_district["type"] == "Okrug"
        else (adm_district["name"], adm_district["id"])
        for adm_district in driver_get_json(URLs.CIAN_DISTRICTS)
        for district in adm_district["childs"]  # oh yeah, perfect english naming
    )


def parsing_iteration() -> None:
    info("Starting parsing iteration...")
    parse_cian_nearby(parse_torgi())
    info("Parsing iteration finished")


def main() -> None:
    setup()

    if REPEAT_DELAY_HOURS:
        schedule.every(REPEAT_DELAY_HOURS).hours.do(parsing_iteration)

        parsing_iteration()

        while True:
            schedule.run_pending()
            try:
                time.sleep(1)
            except KeyboardInterrupt:
                info("exiting")
                return
    else:
        parsing_iteration()


if __name__ == "__main__":
    main()
