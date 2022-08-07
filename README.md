# Media Crawler

A script that uses the New York Times API to get all number one best sellers and also crawls Wikipedia to get Hot 100 number one songs and Box Office number one movies. It then uses the Google Sheets API to save the results to a Google Sheet.

## Set-up

Set-up a virtual environment and activate it:

```bash
python3 -m venv env
source env/bin/activate
```

You should see (env) before your command prompt now. (You can type `deactivate` to exit the virtual environment any time.)

Install the requirements:

```bash
pip install -U pip
pip install -r requirements.txt
```

Obtain a NYT API key [here](https://developer.nytimes.com/get-started).
Follow the instructions [here](https://developers.google.com/workspace/guides/create-project) to create a Google Cloud project and enable the Sheets API
Follow the instructions [here](https://developers.google.com/workspace/guides/create-credentials#oauth-client-id) to create and download OAuth client ID credentials.  Rename this file as `credentials.json` in the main directory.

Set up your environment variables:

```bash
touch .env
echo NYT_API_KEY="XXX" >> .env
echo SPREADSHEET_ID="XXX" >> .env
```

## Usage

Make sure you are in the virtual environment (you should see (env) before your command prompt). If not `source /env/bin/activate` to enter it.

Make sure .env variables are set:

```bash
set -a; source .env; set +a
```

Then run the script:

```bash
Usage: crawler.py
```

## License

Media Crawler is licensed under the [MIT license](https://github.com/danrneal/media-crawler/blob/master/LICENSE).

