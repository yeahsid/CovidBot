import requests
import json
import mysql.connector
import requests_cache
from pymemcache.client import base
import sys
from dotenv import load_dotenv
import os
# API Caching to prevent rate limits . Cache Valid for 4 hours
requests_cache.install_cache(cache_name="api", expire_after=14440)

# API call function . Makes a POST request to https://covid19api.com summary endpoint. This is the source of data
load_dotenv()
HOST = os.getenv("DBHOST")
PORT = os.getenv("DBPORT")
USER = os.getenv("DBUSER")
PASSWD = os.getenv("DBPASSWD")
DATABASE = os.getenv("DBDATABASE")


def api_call():
    try:

        url = "https://api.covid19api.com/summary"
        headers = {}
        payload = {}

        covid19_stats_data = requests.request(
            "GET", url, headers=headers, data=payload)
        print("Using Cache for API call: ", covid19_stats_data.from_cache)
        return covid19_stats_data.json()
    except:
        print(
            "An error occured in fetching data from the API. Are you being rate limited?")
#


def insert_db():

    try:
        covid19db = mysql.connector.connect(
            host=HOST,
            port=PORT,
            user=USER,
            passwd=PASSWD,
            database=DATABASE

        )

    except:
        print("Connection to database failed")

    try:
        data = api_call()
    except:
        print("Could not insert data. Check memcached")
    try:
        cursor = covid19db.cursor()
        sql = 'create table country_stats(ID int NOT NULL AUTO_INCREMENT PRIMARY KEY, Country varchar(30) UNIQUE NOT NULL, CountryCode varchar(5) UNIQUE NOT NULL , Slug varchar(30) UNIQUE NOT NULL, NewConfirmed int  NOT NULL, TotalConfirmed int  NOT NULL, NewDeaths int, TotalDeaths int  NOT NULL, NewRecovered int , TotalRecovered int NOT NULL, Date varchar(30))'
        cursor.execute(sql)
        covid19db.commit()
        print("Table country_stats Created")

    except:
        print("Could not create country_stats Table")
    try:
        cursor = covid19db.cursor()
        sql = 'create table global_stats(ID int NOT NULL AUTO_INCREMENT PRIMARY KEY, NewConfirmed int  NOT NULL, TotalConfirmed int  NOT NULL, NewDeaths int, TotalDeaths int  NOT NULL, NewRecovered int , TotalRecovered int NOT NULL)'
        cursor.execute(sql)
        covid19db.commit()
        print("Table global_stats Created")

    except:
        print("Could not create global_stats Table")
    for i in range(0, len(data["Countries"])):
        Country = data["Countries"][i]["Country"]
        CountryCode = data["Countries"][i]["CountryCode"]
        Slug = data["Countries"][i]["Slug"]
        NewConfirmed = data["Countries"][i]["NewConfirmed"]
        TotalConfirmed = data["Countries"][i]["TotalConfirmed"]
        NewDeaths = data["Countries"][i]["NewDeaths"]
        TotalDeaths = data["Countries"][i]["TotalDeaths"]
        NewRecovered = data["Countries"][i]["NewRecovered"]
        TotalRecovered = data["Countries"][i]["TotalRecovered"]
        Date = data["Countries"][i]["Date"]
        cursor = covid19db.cursor()
        try:

            sql = 'INSERT INTO country_stats (Country, CountryCode, Slug , NewConfirmed , TotalConfirmed , NewDeaths , TotalDeaths , NewRecovered , TotalRecovered , Date) VALUES (%s, %s , %s ,%s, %s , %s ,%s, %s , %s ,%s)'
            val = (Country, CountryCode, Slug, NewConfirmed, TotalConfirmed,
                   NewDeaths, TotalDeaths, NewRecovered, TotalRecovered, Date)
            cursor.execute(sql, val)
            covid19db.commit()

        except:
            print("SQL Error")

    NewConfirmedGlobal = data["Global"]["NewConfirmed"]
    TotalConfirmedGlobal = data["Global"]["TotalConfirmed"]
    NewDeathsGlobal = data["Global"]["NewDeaths"]
    TotalDeathsGlobal = data["Global"]["TotalDeaths"]
    NewRecoveredGlobal = data["Global"]["NewRecovered"]
    TotalRecoveredGlobal = data["Global"]["TotalRecovered"]
    cursor = covid19db.cursor()
    try:

        sql = 'INSERT INTO global_stats (NewConfirmed , TotalConfirmed , NewDeaths , TotalDeaths , NewRecovered , TotalRecovered ) VALUES (%s, %s , %s ,%s, %s , %s )'
        val = (NewConfirmedGlobal, TotalConfirmedGlobal,
               NewDeathsGlobal, TotalDeathsGlobal, NewRecoveredGlobal, TotalRecoveredGlobal)
        cursor.execute(sql, val)
        covid19db.commit()

    except:
        print("Could not insert into global_stats", sys.exc_info()[1])
    try:
        cursor.close()
        print("Connection closed successfully")
    except:
        print("Cannot close DB Connection", sys.exc_info()[1])


def update_db():
    data = api_call()

    try:
        covid19db = mysql.connector.connect(
            host=HOST,
            port=PORT,
            user=USER,
            passwd=PASSWD,
            database=DATABASE
        )

    except:
        print("Connection to database failed", sys.exc_info()[1])
    try:
        cursor = covid19db.cursor()
        sql = 'TRUNCATE TABLE country_stats'
        cursor.execute(sql)
        covid19db.commit()
        print("country_stats Table deleted , inserting data")
    except:
        print("Table couldnt be deleted", sys.exc_info()[1])
    try:
        cursor = covid19db.cursor()
        sql = 'TRUNCATE TABLE global_stats'
        cursor.execute(sql)
        covid19db.commit()
        print("global_stats Table deleted , inserting data")
    except:
        print("Table couldnt be deleted", sys.exc_info()[1])

    for i in range(0, len(data["Countries"])):

        Country = data["Countries"][i]["Country"]
        CountryCode = data["Countries"][i]["CountryCode"]
        Slug = data["Countries"][i]["Slug"]
        NewConfirmed = data["Countries"][i]["NewConfirmed"]
        TotalConfirmed = data["Countries"][i]["TotalConfirmed"]
        NewDeaths = data["Countries"][i]["NewDeaths"]
        TotalDeaths = data["Countries"][i]["TotalDeaths"]
        NewRecovered = data["Countries"][i]["NewRecovered"]
        TotalRecovered = data["Countries"][i]["TotalRecovered"]
        Date = data["Countries"][i]["Date"]

        try:

            sql = 'INSERT INTO country_stats (Country, CountryCode, Slug , NewConfirmed , TotalConfirmed , NewDeaths , TotalDeaths , NewRecovered , TotalRecovered , Date) VALUES (%s, %s , %s ,%s, %s , %s ,%s, %s , %s ,%s)'
            val = (Country, CountryCode, Slug, NewConfirmed, TotalConfirmed,
                   NewDeaths, TotalDeaths, NewRecovered, TotalRecovered, Date)
            cursor.execute(sql, val)
            covid19db.commit()

        except:
            print("Could not insert country_stats", sys.exc_info()[1])

    NewConfirmedGlobal = data["Global"]["NewConfirmed"]
    TotalConfirmedGlobal = data["Global"]["TotalConfirmed"]
    NewDeathsGlobal = data["Global"]["NewDeaths"]
    TotalDeathsGlobal = data["Global"]["TotalDeaths"]
    NewRecoveredGlobal = data["Global"]["NewRecovered"]
    TotalRecoveredGlobal = data["Global"]["TotalRecovered"]
    cursor = covid19db.cursor()
    try:

        sql = 'INSERT INTO global_stats (NewConfirmed , TotalConfirmed , NewDeaths , TotalDeaths , NewRecovered , TotalRecovered ) VALUES (%s, %s , %s ,%s, %s , %s )'
        val = (NewConfirmedGlobal, TotalConfirmedGlobal,
               NewDeathsGlobal, TotalDeathsGlobal, NewRecoveredGlobal, TotalRecoveredGlobal)
        cursor.execute(sql, val)
        covid19db.commit()

    except:
        print("Could not insert global_stats values", sys.exc_info()[1])
    try:
        cursor.close()
        print("Connection closed successfully")
    except:
        print("Cannot close DB Connection", sys.exc_info()[1])


update_db()
