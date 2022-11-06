from app.entities.wrapped import Wrapped
from app.entities.trend import Trend
from app.entities.popularity import Popularity
from app.entities.keyword import Keyword
from app.entities.sentiment import Sentiment
# Twitter API
import tweepy
import os
import json
from operator import itemgetter
# Dot Env
from dotenv import load_dotenv
from pathlib import Path
# Date
from datetime import datetime, timedelta
from pytz import timezone
# Request
import requests
# News Extractor
from app.data.news_extractor import get_everything
from app.data.ml_extractor import fetch_tweets, fetch_sentiments
import concurrent.futures
import string


dotenv_path = Path('../../.env')
load_dotenv()


# ############################## GLOBAL ENTITIES ###############################

# NEWS API AND TWITTER API
consumer_key = os.getenv('API_KEY')
consumer_secret = os.getenv('API_SECRET')
access_token = os.getenv('ACCESS_TOKEN')
access_token_secret = os.getenv('ACCESS_TOKEN_SECRET')


# TWITTER API
auth = tweepy.OAuthHandler(consumer_key, consumer_secret)
auth.set_access_token(access_token, access_token_secret)
api = tweepy.API(auth)

# client = tweepy.Client(bearer_token=BEARER_TOKEN)

# GLOBAL VARIABLES
COUNTRIES = [
    {
        'name': 'Spain',
        'woeid': 23424950,
        'code': 'ES',
        'language': 'es',
        'timezone': 'Europe/Madrid'
    },
    {
        'name': 'United States',
        'woeid': 23424977,
        'code': 'US',
        'language': 'en',
        'timezone': 'US/Pacific'
    },
    {
        'name': 'Ukraine',
        'woeid': 23424976,
        'code': 'UA',
        'language': 'uk',
        'timezone': 'Europe/Kiev'
    }
]

TRENDING_WRAPPED_ENDPOINT = "https://wrappedtrends.herokuapp.com/country/"


# ################################# FUNCTIONS ###################################

# PARSING OBJECT TREND
def parse_popularity(vol, time):

    values = {
        'volume': [vol],
        'hour_popularity': [time],
        'avarage_popularity': vol,
        'peak_popularity': vol,
    }

    return Popularity(**values)


# PARSING OBJECT KEYWORD
def parse_keyword(k):

    values = {
        'total': k['total'],
        'count': k['count'],
        'label': k['label'],
        'words': k['words'],
    }

    return Keyword(**values)


# PARSING OBJECT SENTIMENT
def parse_sentiment(s):

    values = {
        'positive': s['positive'],
        'negative': s['negative'],
        'neutral': s['neutral'],
    }

    return Sentiment(**values)


# PARSING OBJECT TREND
def parse_trend(name, vol, time, array):

    popularity = parse_popularity(vol, time)

    values = {
        'name': name,
        'popularity': popularity,
        'wrappeds': array
    }

    return Trend(**values)


def parse_single_trend(name, array):

    values = {
        'name': name,
        'popularity': None,
        'wrappeds': array
    }

    return Trend(**values)


# PARSING OBJECT WRAPPED
def parse_wrapped(item):

    date = item['date']
    image = item['image']
    summary = item['summary']
    title = item['title']
    url = item['url']
    source = item['source']

    values = {
        'date': date,
        'image': image,
        'summary': summary,
        'title': title,
        'url': url,
        'source': source
    }

    return Wrapped(**values)


def normalize_str(c):
    s = string.punctuation + ' '
    return c.translate(str.maketrans('áéíóúüàèìòùç', 'aeiouuaeiouc', s)).lower()


# GETTING TRENDS WITH TWITTER API
def get_trends_with_country(woeid):

    try:

        # Specific Country by WOEID
        trends = api.get_place_trends(woeid, exclude="hashtags")
        trends = trends[0]["trends"]
        # Removing trends with no Tweet volume data
        trends = list(filter(itemgetter("tweet_volume"), trends))

        return trends

    except Exception as e:

        print(e)
        return False


def get_wrappeds(name, code, lang):

    wrappeds = []
    news = get_everything(f'{name} when:3d', code, lang)
    if news:
        for n in news:
            wrapped = parse_wrapped(n)
            wrappeds.append(wrapped)

        return wrappeds
    else:
        return False


def get_tweets(tweets, lng):

    t = fetch_tweets(tweets, lng)
    if t:
        t = parse_keyword(t)
        return t
    else:
        return False


def get_sentiments(tweets, lng, kw):

    s = fetch_sentiments(tweets, lng, kw)
    if s:
        s = parse_sentiment(s)
        return s
    else:
        return False


def more_specific_name(words, name):

    list_words = []
    text = ""
    for item in words:
        w = item.split()
        list_words += w
    w = name.translate(str.maketrans('áéíóúüàèìòùç', 'aeiouuaeiouc')).lower()
    w = w.split()
    list_words += w
    text += f"{' '.join(sorted(set(list_words), key=list_words.index))}"

    return text


def comparing_names(s):
    i = 0
    while i < (len(s)-1):
        j = i+1
        while j < (len(s)):
            if normalize_str(s[j].name) in normalize_str(s[i].name) \
                    or normalize_str(s[i].name) in normalize_str(s[j].name):
                if len((s[i].name).split()) > len((s[j].name).split()):
                    s.pop(j)
                    j -= 1
                elif len((s[i].name).split()) < len((s[j].name).split()):
                    s.pop(i)
                    i -= 1
                    break
                else:
                    if s[j].popularity.peak_popularity >= s[i].popularity.peak_popularity:
                        s.pop(i)
                        i -= 1
                        break
                    else:
                        s.pop(j)
                        j -= 1
            j += 1
        i += 1

    return s


# GETTING ARTICLES WITH NEWS SCRAPING
def create_single_trend(country, name):

    try:

        sorted_parsed = []
        code = None
        for c in COUNTRIES:
            if country == c['name']:
                code = c['code']
                language = c['language']
                time = c['timezone']
                break

        if code:

            date = datetime.now(timezone(time)).strftime("%y-%m-%d")    # type: ignore [arg-type]
            yesterday = (datetime.now(timezone(time)) -                 # type: ignore [arg-type]
                         timedelta(days=2)).strftime("%Y-%m-%d")

            t = parse_single_trend(name, None)
            sorted_parsed.append(t)

            # Setting the news !

            name = sorted_parsed[0].name
            tweets = api.search_tweets(q=f'{name} -filter:retweets min_faves:20 since:{yesterday}',
                                       lang=language, count=100, result_type="mixed",
                                       tweet_mode="extended")
            if (len(tweets)) < 50:
                tweets = api.search_tweets(q=f'{name} -filter:retweets since:{yesterday}',
                                           lang=language, count=100, result_type="mixed",
                                           tweet_mode="extended")
            t = get_tweets(tweets, language)
            if t:
                name = more_specific_name(t.words, name)
                w = get_wrappeds(name, code, language)
                if w:
                    s = get_sentiments(tweets, language, t.label)
                    if s:
                        sorted_parsed[0].wrappeds = w
                        sorted_parsed[0].keywords = t
                        sorted_parsed[0].sentiments = s

            dict_country = {
                'pais': country,
                'dia': date,
                'tendencias': sorted_parsed
            }

            return dict_country
        else:
            return False

    except Exception as e:

        print(e)
        return False


# GETTING ARTICLES WITH NEWS SCRAPING
def create_dict_country(country, sorted_trends, date, yesterday, old_trends):

    try:

        sorted_parsed = []
        hour = datetime.now(timezone(country["timezone"])).strftime("%Y-%m-%dT%H:00:00.000Z")
        cont = 0

        # When there are some trends, so we have to do the Union and also the Symmetric Difference
        if old_trends:
            for item in sorted_trends:
                t = parse_trend(item['name'], item['tweet_volume'], hour, None)
                for k, old_item in enumerate(old_trends):
                    if old_item["name"] == item["name"]:
                        t.popularity.refresh(old_item["popularity"]["volume"],
                                             old_item["popularity"]["hour_popularity"])
                        old_trends.pop(k)
                        break
                sorted_parsed.append(t)
            for i in old_trends:
                sorted_parsed.append(Trend(**i))

            sorted_parsed = comparing_names(sorted_parsed)
            # Ignoring error of mypy because it's caused by sorted, external error
            sorted_parsed = sorted(sorted_parsed, key=lambda trend: trend.popularity.peak_popularity, reverse=True)[
                :20]  # type: ignore [no-any-return]

        # When there aren't any new trends on the platfrom yet
        else:
            for item in sorted_trends:
                t = parse_trend(item['name'], item['tweet_volume'], hour, None)
                sorted_parsed.append(t)
            sorted_parsed = comparing_names(sorted_parsed)
            sorted_trends = sorted(sorted_trends, key=itemgetter("tweet_volume"), reverse=True)[:20]

        # Setting the news !
        wts = 0
        while wts < 10 and cont < len(sorted_parsed):
            name = sorted_parsed[cont].name
            tweets = api.search_tweets(q=f'{name} -filter:retweets min_faves:20 since:{yesterday}',
                                       lang=country["language"], count=100, result_type="mixed",
                                       tweet_mode="extended")
            if (len(tweets)) < 50:
                tweets = api.search_tweets(q=f'{name} -filter:retweets since:{yesterday}',
                                           lang=country["language"], count=100, result_type="mixed",
                                           tweet_mode="extended")
            t = get_tweets(tweets, country["language"])
            if t:
                name = more_specific_name(t.words, name)
                w = get_wrappeds(name, country["code"], country["language"])
                if w:
                    s = get_sentiments(tweets, country["language"], t.label)
                    if s:
                        sorted_parsed[cont].wrappeds = w
                        sorted_parsed[cont].keywords = t
                        sorted_parsed[cont].sentiments = s
                        wts += 1
            cont += 1

        dict_country = {
            'pais': country["name"],
            'dia': date,
            'tendencias': sorted_parsed
        }

        return dict_country

    except Exception as e:

        print(e)
        return False


def main_operation(country):

    sorted_trends = get_trends_with_country(country['woeid'])
    if sorted_trends:

        DATE = datetime.now(timezone(country['timezone'])).strftime("%y-%m-%d")
        YESTERDAY = (datetime.now(timezone(country['timezone'])) - timedelta(days=2)).strftime("%Y-%m-%d")
        resp = requests.get(f"{TRENDING_WRAPPED_ENDPOINT}{country['name']}/{DATE}")

        # Checking if the object exists already
        if resp.status_code == 200:
            my_dict = create_dict_country(country, sorted_trends, DATE, YESTERDAY,
                                          json.loads(resp.content)["tendencias"])

        else:
            my_dict = create_dict_country(country, sorted_trends, DATE, YESTERDAY, False)

        if my_dict:
            my_dict_json = json.dumps(my_dict, default=lambda o: o.__dict__)
            my_headers = {
                'accept': 'application/json',
                'Content-Type': 'application/json'
            }
            # Checking if the object exists already
            if resp.status_code == 200:
                req = requests.put(TRENDING_WRAPPED_ENDPOINT, data=my_dict_json, headers=my_headers)
            else:
                req = requests.post(TRENDING_WRAPPED_ENDPOINT, data=my_dict_json, headers=my_headers)
                OLD_DATE = (datetime.now(timezone(country['timezone'])) - timedelta(days=7)).strftime("%y-%m-%d")
                old_resp = requests.get(f"{TRENDING_WRAPPED_ENDPOINT}{country['name']}/{OLD_DATE}")
                if old_resp.status_code == 200:
                    old_resp = requests.delete(f"{TRENDING_WRAPPED_ENDPOINT}{country['name']}/{OLD_DATE}")
            print(req.text)

    # with open("db_trend.json", 'w+') as json_file:
    #    json.dump(trends, json_file, sort_keys=True, indent=4, default=lambda o: o.__dict__)


# #################################### MAIN #####################################

def main():

    # Multiprocessing
    executor = concurrent.futures.ProcessPoolExecutor(200)
    futures = [executor.submit(main_operation, country) for country in COUNTRIES]
    concurrent.futures.wait(futures)


def test():

    (create_single_trend("Spain", "Young Royals"))


if __name__ == "__main__":

    main()
