import os
import re
from datetime import datetime

import requests
from dateutil.parser import parse as dateparse
from google.auth.transport.requests import Request
from google.oauth2.credentials import Credentials
from google_auth_oauthlib.flow import InstalledAppFlow
from googleapiclient.discovery import build
from ratelimit import limits, sleep_and_retry
from requests_html import HTMLSession
from titlecase import titlecase

SHEET_ID = os.environ["SPREADSHEET_ID"]
SHEETS_RATE_LIMIT = 50
NYT_START_DATE = "2011-02-13"
NYT_API_KEY = os.environ["NYT_API_KEY"]
NYT_RATE_LIMIT = 10
HOT_100_START_DATE = "2008-03-15"
BOX_OFFICE_START_DATE = "2007-03-11"
EXCEPTIONS = [
    {
        "film": "Halloween",
        "date": "2018-10-21",
    },
    {
        "film": "The Lion King",
        "date": "2019-07-21",
    },
]


class Sheets:
    def __init__(self, sheet_id):
        self.sheet_id = sheet_id
        self.sheet = self._create_sheets_service()
        self.headers = {}

    def _create_sheets_service(self):
        creds = None
        scopes = ["https://www.googleapis.com/auth/spreadsheets"]
        if os.path.exists("token.json"):
            creds = Credentials.from_authorized_user_file("token.json", scopes)

        if not creds or not creds.valid:
            if creds and creds.expired and creds.refresh_token:
                creds.refresh(Request())
            else:
                flow = InstalledAppFlow.from_client_secrets_file(
                    "credentials.json", scopes
                )
                creds = flow.run_local_server(port=0)

            with open("token.json", "w", encoding="utf8") as token:
                token.write(creds.to_json())

        service = build("sheets", "v4", credentials=creds)
        sheet = service.spreadsheets()

        return sheet

    def load_sheet(self, sheet_name):
        data = []
        result = (
            self.sheet.values()
            .get(spreadsheetId=self.sheet_id, range=sheet_name)
            .execute()
        )
        rows = result.get("values")
        headers = [header.lower() for header in rows[0]]
        self.headers[sheet_name] = headers
        for row in rows[1:]:
            row.extend([""] * (len(headers) - len(row)))
            data.append(dict(zip(headers, row)))

        return data

    @sleep_and_retry
    @limits(calls=SHEETS_RATE_LIMIT, period=60)
    def append_to_sheet(self, data, sheet_name):
        headers = self.headers[sheet_name]
        values = []
        for row in data:
            values.append([row.get(header, "") for header in headers])

        payload = {"values": values}
        self.sheet.values().append(
            spreadsheetId=self.sheet_id,
            range=sheet_name,
            valueInputOption="USER_ENTERED",
            body=payload,
        ).execute()


def main():
    sheets = Sheets(SHEET_ID)
    books = sheets.load_sheet("Books")
    get_nyt_best_sellers(books, sheets)
    movies = sheets.load_sheet("Movies")
    get_box_office_number_ones(movies, sheets)
    music = sheets.load_sheet("Music")
    get_hot_100_number_ones(music, sheets)


def get_nyt_best_sellers(books, sheets):
    book_cache = set()
    published_date = NYT_START_DATE
    for book in books:
        book_cache.add((book["title"], book["author"]))
        published_date = max(published_date, book["date"])

    n = 0
    books = []
    while published_date:
        best_sellers, published_date = get_nyt_overview(published_date)
        for best_seller in best_sellers:
            if (best_seller["title"], best_seller["author"]) not in book_cache:
                book_cache.add((best_seller["title"], best_seller["author"]))
                books.append(best_seller)

        n += 1
        if n % NYT_RATE_LIMIT == 0 or not published_date:
            sheets.append_to_sheet(books, "Books")
            books = []


@sleep_and_retry
@limits(calls=NYT_RATE_LIMIT, period=60)
def get_nyt_overview(published_date):
    best_sellers = []
    print(f"Getting books from NYT Best Sellers list from {published_date}")
    url = (
        "https://api.nytimes.com/svc/books/v3/lists/full-overview.json"
        f"?published_date={published_date}&api-key={NYT_API_KEY}"
    )
    response = requests.get(url).json()
    results = response["results"]
    for list_ in results["lists"][:2]:
        best_seller = list_["books"][0]
        best_sellers.append(
            {
                "title": titlecase(best_seller["title"]),
                "author": best_seller["author"],
                "date": results["published_date"],
                "category": list_["list_name"].split()[-1],
            }
        )

    return best_sellers, results["next_published_date"]


def get_box_office_number_ones(movies, sheets):
    movie_cache = set()
    weekend_end_date = BOX_OFFICE_START_DATE
    for movie in movies:
        movie_cache.add(movie["film"])
        weekend_end_date = max(weekend_end_date, movie["date"])

    year = dateparse(weekend_end_date).year
    session = HTMLSession()
    movies = []
    while year <= datetime.now().year:
        number_ones = scrape_box_office_wiki(year, session)
        for number_one in number_ones:
            if number_one["date"] >= weekend_end_date and (
                number_one["film"] not in movie_cache
                or number_one in EXCEPTIONS
            ):
                movie_cache.add(number_one["film"])
                movies.append(number_one)

        year += 1

    sheets.append_to_sheet(movies, "Movies")


def scrape_box_office_wiki(year, session):
    number_ones = []
    print(f"Getting movies from Wiki Box Office list from {year}")
    url = (
        "https://en.wikipedia.org/wiki/"
        f"List_of_{year}_box_office_number-one_films_in_the_United_States"
    )
    response = session.get(url).html
    rows = response.find("table", first=True).find("tr")
    date_rowspan = 1
    film_rowspan = 1
    for row in rows[1:]:
        values = row.find("th, td")

        # Skip the row if there is a colspan like 2020-03-22
        if values[2].attrs.get("colspan"):
            continue

        if date_rowspan == 1:
            date_val = values[1]
            date_rowspan = int(date_val.attrs.get("rowspan", 1))
            weekend_end_date = date_val.text.strip()
            date = dateparse(weekend_end_date).date().strftime("%Y-%m-%d")
        else:
            date_rowspan -= 1

        if film_rowspan == 1:
            film_val = values[2]
            film_rowspan = int(film_val.attrs.get("rowspan", 1))
            film = film_val.find("i", first=True).text.strip()
            number_ones.append(
                {
                    "date": date,
                    "film": film,
                }
            )
        else:
            film_rowspan -= 1

    return number_ones


def get_hot_100_number_ones(music, sheets):
    song_cache = set()
    issue_date = HOT_100_START_DATE
    for song in music:
        song_cache.add((song["song"], song["artist"]))
        issue_date = max(issue_date, song["date"])

    year = dateparse(issue_date).year
    session = HTMLSession()
    music = []
    while year <= datetime.now().year:
        number_ones = scrape_hot_100_wiki(year, session)
        for number_one in number_ones:
            if (
                number_one["date"] >= issue_date
                and (number_one["song"], number_one["artist"])
                not in song_cache
            ):
                song_cache.add((number_one["song"], number_one["artist"]))
                music.append(number_one)

        year += 1

    sheets.append_to_sheet(music, "Music")


def scrape_hot_100_wiki(year, session):
    number_ones = []
    print(f"Getting music from Wiki Hot 100 list from {year}")
    url = (
        "https://en.m.wikipedia.org/wiki/"
        f"List_of_Billboard_Hot_100_number_ones_of_{year}"
    )
    response = session.get(url).html
    rows = response.find("table")[-1].find("tr")
    song_rowspan = 1
    artist_rowspan = 1
    for row in rows[1:]:
        values = row.find("th, td")

        if artist_rowspan == 1 and song_rowspan == 1:
            artist_val = values[3]
            artist_rowspan = int(artist_val.attrs.get("rowspan", 1))
            artist = re.sub(r"\[.*?\]", "", artist_val.text).strip()
        elif artist_rowspan > 1:
            artist_rowspan -= 1

        if song_rowspan == 1:
            date_val = values[1]
            weekend_end_date = f"{date_val.text.strip()}, {year}"
            date = dateparse(weekend_end_date).date().strftime("%Y-%m-%d")
            song_val = values[2]
            song_rowspan = int(song_val.attrs.get("rowspan", 1))
            song = song_val.text.split('"')[1].strip()
            number_ones.append(
                {
                    "date": date,
                    "song": song,
                    "artist": artist,
                }
            )
        else:
            song_rowspan -= 1

    return number_ones


if __name__ == "__main__":
    main()
