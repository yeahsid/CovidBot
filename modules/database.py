import requests
import json
import mysql.connector
import requests_cache
import sys
from dotenv import load_dotenv
import os
from fetch import apiCall
#import modules.fetch

# API Caching to prevent rate limits . Cache Valid for 4 hours
requests_cache.install_cache(cache_name="api", expire_after=14440)


load_dotenv()
HOST = os.getenv("DBHOST")
PORT = os.getenv("DBPORT")
USER = os.getenv("DBUSER")
PASSWD = os.getenv("DBPASSWD")
DATABASE = os.getenv("DBDATABASE")
MEMPORT = os.getenv("MEMPORT")


# API call function . Makes a POST request to https://covid19api.com summary endpoint. This is the source of data


#


def insertDb():

    try:
        covid19db = mysql.connector.connect(
            host=HOST,
            port=PORT,
            user=USER,
            passwd=PASSWD,
            database=DATABASE

        )

    except:
        raise ConnectionError("Connection to database failed")

    try:
        data = apiCall()
    except:
        raise ConnectionError("Fetch failed. Check URL. [insertdb]")
    try:
        cursor = covid19db.cursor()
        sql = 'create table countryStats(ID int NOT NULL AUTO_INCREMENT PRIMARY KEY, country varchar(30) UNIQUE NOT NULL, countryCode varchar(5) UNIQUE NOT NULL , slug varchar(30) UNIQUE NOT NULL, dailyNewConfirmed int  NOT NULL, totalConfirmed int  NOT NULL, dailyNewDeaths int, totalDeaths int  NOT NULL, dailyNewRecovered int , totalRecovered int NOT NULL, date varchar(30))'
        cursor.execute(sql)
        covid19db.commit()

    except:
        raise Exception("Cannot create table countryStats")
    try:
        cursor = covid19db.cursor()
        sql = 'create table globalStats(ID int NOT NULL AUTO_INCREMENT PRIMARY KEY, dailyNewConfirmed int  NOT NULL, totalConfirmed int  NOT NULL, dailyNewDeaths int, totalDeaths int  NOT NULL, dailyNewRecovered int , totalRecovered int NOT NULL)'
        cursor.execute(sql)
        covid19db.commit()

    except:
        raise Exception("Could not create globalStats Table")
    for i in range(0, len(data["Countries"])):
        country = data["Countries"][i]["Country"]
        countryCode = data["Countries"][i]["CountryCode"]
        slug = data["Countries"][i]["Slug"]
        dailyNewConfirmed = data["Countries"][i]["NewConfirmed"]
        totalConfirmed = data["Countries"][i]["TotalConfirmed"]
        dailyNewDeaths = data["Countries"][i]["NewDeaths"]
        totalDeaths = data["Countries"][i]["TotalDeaths"]
        dailyNewRecovered = data["Countries"][i]["NewRecovered"]
        totalRecovered = data["Countries"][i]["TotalRecovered"]
        date = data["Countries"][i]["Date"]
        cursor = covid19db.cursor()
        try:

            sql = 'INSERT INTO countryStats (country, countryCode, slug , dailyNewConfirmed , totalConfirmed , dailyNewDeaths , totalDeaths , dailyNewRecovered , totalRecovered , date) VALUES (%s, %s , %s ,%s, %s , %s ,%s, %s , %s ,%s)'
            val = (country, countryCode, slug, dailyNewConfirmed, totalConfirmed,
                   dailyNewDeaths, totalDeaths, dailyNewRecovered, totalRecovered, date)
            cursor.execute(sql, val)
            covid19db.commit()

        except:
            raise Exception("Could not insert data into countryStats table")

    dailyNewConfirmedGlobal = data["Global"]["NewConfirmed"]
    totalConfirmedGlobal = data["Global"]["TotalConfirmed"]
    dailyNewDeathsGlobal = data["Global"]["NewDeaths"]
    totalDeathsGlobal = data["Global"]["TotalDeaths"]
    dailyNewRecoveredGlobal = data["Global"]["NewRecovered"]
    totalRecoveredGlobal = data["Global"]["TotalRecovered"]
    cursor = covid19db.cursor()
    try:

        sql = 'INSERT INTO globalStats (dailyNewConfirmed , totalConfirmed , dailyNewDeaths , totalDeaths , dailyNewRecovered , totalRecovered ) VALUES (%s, %s , %s ,%s, %s , %s )'
        val = (dailyNewConfirmedGlobal, totalConfirmedGlobal,
               dailyNewDeathsGlobal, totalDeathsGlobal, dailyNewRecoveredGlobal, totalRecoveredGlobal)
        cursor.execute(sql, val)
        covid19db.commit()

    except:
        raise Exception("Could not insert data  into  globalStats Table")
    try:
        cursor.close()
    except:
        raise ConnectionError("Cannot close DB Connection")


def updateDb():
    data = apiCall()

    try:
        covid19db = mysql.connector.connect(
            host=HOST,
            port=PORT,
            user=USER,
            passwd=PASSWD,
            database=DATABASE
        )

    except:
        raise ConnectionError("Connection to database failed")
    try:
        cursor = covid19db.cursor()
        sql = 'TRUNCATE TABLE countryStats'
        cursor.execute(sql)
        covid19db.commit()
    except:
        raise Exception("Table countryStats couldnt be truncated")
    try:
        cursor = covid19db.cursor()
        sql = 'TRUNCATE TABLE globalStats'
        cursor.execute(sql)
        covid19db.commit()
    except:
        raise Exception("Table globalStats couldnt be truncated")

    for i in range(0, len(data["Countries"])):

        country = data["Countries"][i]["Country"]
        countryCode = data["Countries"][i]["CountryCode"]
        slug = data["Countries"][i]["Slug"]
        dailyNewConfirmed = data["Countries"][i]["NewConfirmed"]
        totalConfirmed = data["Countries"][i]["TotalConfirmed"]
        dailyNewDeaths = data["Countries"][i]["NewDeaths"]
        totalDeaths = data["Countries"][i]["TotalDeaths"]
        dailyNewRecovered = data["Countries"][i]["NewRecovered"]
        totalRecovered = data["Countries"][i]["TotalRecovered"]
        date = data["Countries"][i]["Date"]

        try:

            sql = 'INSERT INTO countryStats (country, countryCode, slug , dailyNewConfirmed , totalConfirmed , dailyNewDeaths , totalDeaths , dailyNewRecovered , totalRecovered , date) VALUES (%s, %s , %s ,%s, %s , %s ,%s, %s , %s ,%s)'
            val = (country, countryCode, slug, dailyNewConfirmed, totalConfirmed,
                   dailyNewDeaths, totalDeaths, dailyNewRecovered, totalRecovered, date)
            cursor.execute(sql, val)
            covid19db.commit()

        except:
            raise Exception("Could not update data in countryStats")

    dailyNewConfirmedGlobal = data["Global"]["NewConfirmed"]
    totalConfirmedGlobal = data["Global"]["TotalConfirmed"]
    dailyNewDeathsGlobal = data["Global"]["NewDeaths"]
    totalDeathsGlobal = data["Global"]["TotalDeaths"]
    dailyNewRecoveredGlobal = data["Global"]["NewRecovered"]
    totalRecoveredGlobal = data["Global"]["TotalRecovered"]
    cursor = covid19db.cursor()
    try:

        sql = 'INSERT INTO globalStats (dailyNewConfirmed , totalConfirmed , dailyNewDeaths , totalDeaths , dailyNewRecovered , totalRecovered ) VALUES (%s, %s , %s ,%s, %s , %s )'
        val = (dailyNewConfirmedGlobal, totalConfirmedGlobal,
               dailyNewDeathsGlobal, totalDeathsGlobal, dailyNewRecoveredGlobal, totalRecoveredGlobal)
        cursor.execute(sql, val)
        covid19db.commit()

    except:
        raise Exception("Could not insert globalStats values")
    try:
        cursor.close()
        print("Connection closed successfully")
    except:
        raise ConnectionError("Cannot close DB Connection")
