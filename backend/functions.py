import requests
import json
import mysql.connector
import requests_cache
import sys
from dotenv import load_dotenv
import os
import ast
from pymemcache.client import base
#import modules.fetch

# API Caching to prevent rate limits . Cache Valid for 4 hours
requests_cache.install_cache(cache_name="cache/api", expire_after=14440)


load_dotenv()
HOST = os.getenv("DBHOST")
PORT = os.getenv("DBPORT")
USER = os.getenv("DBUSER")
PASSWD = os.getenv("DBPASSWD")
DATABASE = os.getenv("DBDATABASE")
MEMPORT = os.getenv("MEMPORT")
client = base.Client(MEMPORT)


# API call function . Makes a POST request to https://covid19api.com summary endpoint. This is the source of data


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
        sql = 'create table countryStats(ID int NOT NULL AUTO_INCREMENT PRIMARY KEY, country varchar(50) UNIQUE NOT NULL, countryCode varchar(5) UNIQUE NOT NULL , slug varchar(50) UNIQUE NOT NULL, dailyNewConfirmed int  NOT NULL, totalConfirmed int  NOT NULL, dailyNewDeaths int, totalDeaths int  NOT NULL, dailyNewRecovered int , totalRecovered int NOT NULL, date varchar(30))'
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


def apiCall():
    try:

        url = "https://api.covid19api.com/summary"
        headers = {}
        payload = {}

        covid19StatsData = requests.request(
            "GET", url, headers=headers, data=payload)
        #print("Using Cache for API call: ", covid19_stats_data.from_cache)
        return covid19StatsData.json()
    except:
        #print("An error occured in fetching data from the API. Are you being rate limited?")
        raise ConnectionError("Fetch failed. Check URL")


def getCountryStats(slug):

    try:

        data = client.get(slug)
        if data is None:
            try:

                covid19db = mysql.connector.connect(
                    host=HOST,
                    port=PORT,
                    user=USER,
                    passwd=PASSWD,
                    database=DATABASE
                )
                cursor = covid19db.cursor(dictionary=True)
                try:
                    sql = "SELECT * from countryStats WHERE slug = %s"
                    val = (slug, )
                    cursor.execute(sql, val)
                    result = cursor.fetchone()
                    print("Using DB call with Memcached")
                    if result is None:
                        return result
                    else:
                        client.set(slug,
                                   result, expire=14440)
                        return result
                except:
                    raise Exception("Could not execute the SQL Statement")
            except:
                try:

                    data = apiCall()
                    for i in range(0, len(data["Countries"])):
                        countryAPI = data["Countries"][i]["Slug"]
                        if countryAPI == slug:

                            result = data["Countries"][i]
                            client.set(slug, result,  expire=14440)
                            return result
                except:
                    raise Exception("API call failed")
        tempData = data.decode("UTF-8")
        mydata = ast.literal_eval(tempData)
        return mydata

    except:
        try:
            covid19db = mysql.connector.connect(
                host=HOST,
                port=PORT,
                user=USER,
                passwd=PASSWD,
                database=DATABASE
            )
            try:
                cursor = covid19db.cursor(dictionary=True)
                sql = "SELECT * from countryStats WHERE slug = %s"
                val = (slug, )
                cursor.execute(sql, val)
                data = cursor.fetchone()
                if data is None:
                    print("country not found")
                else:
                    return data
            except:
                raise Exception("SQL Execution failed")
        except:
            try:
                print("Memcached and database are down . Using API directly")
                data1 = apiCall()
                for i in range(0, len(data1["Countries"])):
                    countryAPI = data1["Countries"][i]["Slug"]
                    if countryAPI == slug:

                        data = data1["Countries"][i]
                        return data

            except:
                raise Exception("All methods to fetch data have failed")


def getStats():

    try:
        data = client.get('Global')
        if data is None:
            try:

                covid19db = mysql.connector.connect(
                    host=HOST,
                    port=PORT,
                    user=USER,
                    passwd=PASSWD,
                    database=DATABASE
                )
                cursor = covid19db.cursor(dictionary=True)
                try:
                    sql = "SELECT * from globalStats "
                    cursor.execute(sql)
                    result = cursor.fetchone()
                    print("Using DB call with Memcached")
                    client.set("Global", result, expire=14440)
                    return result
                except:
                    raise Exception("Could not execute the SQL Statement")
            except:
                try:
                    data = apiCall()
                    client.set("Global", result, expire=14440)
                    return data
                except:
                    raise Exception("API call failed")

        tempData = data.decode("UTF-8")
        mydata = ast.literal_eval(tempData)
        return mydata

    except Exception:
        try:
            covid19db = mysql.connector.connect(
                host=HOST,
                port=PORT,
                user=USER,
                passwd=PASSWD,
                database=DATABASE
            )
            try:
                cursor = covid19db.cursor(dictionary=True)
                sql = "SELECT * from globalStats"
                cursor.execute(sql)
                data = cursor.fetchone()
                return data
            except:
                raise Exception("SQL Execution failed")
        except:
            try:
                data = apiCall()
                return data

            except:
                raise Exception("All methods to fetch data have failed")
