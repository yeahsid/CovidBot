import mysql.connector
import pylibmc
from dotenv import load_dotenv
import os
import sys
import requests
sys.path.append('modules/')

load_dotenv()
HOST = os.getenv("DBHOST")
PORT = os.getenv("DBPORT")
USER = os.getenv("DBUSER")
PASSWD = os.getenv("DBPASSWD")
DATABASE = os.getenv("DBDATABASE")
MEMPORT = os.getenv("MEMPORT")
client = pylibmc.Client([MEMPORT])


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


def getCountryStats(country):

    try:

        data = client.get(country.replace(" ", ""))
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
                    sql = "SELECT * from countryStats WHERE country = %s"
                    val = (country, )
                    cursor.execute(sql, val)
                    result = cursor.fetchone()
                    print("Using DB call with Memcached")
                    if result is None:
                        return result
                    else:
                        client.set(country.replace(" ", ""), result, time=14440)
                        return result
                except:
                    raise Exception("Could not execute the SQL Statement")
            except:
                try:

                    data = apiCall()
                    for i in range(0, len(data["Countries"])):
                        countryAPI = data["Countries"][i]["Country"]
                        if countryAPI == country:

                            result = data["Countries"][i]
                            client.set(country, result, time=14440)
                            return result
                except:
                    raise Exception("API call failed")
        return data

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
                sql = "SELECT * from countryStats WHERE country = %s"
                val = (country, )
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
                    countryAPI = data1["Countries"][i]["Country"]
                    if countryAPI == country:

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
                    client.set("Global", result, time=14440)
                    return result
                except:
                    raise Exception("Could not execute the SQL Statement")
            except:
                try:
                    data = apiCall()
                    client.set("Global", result, time=14440)
                    return data
                except:
                    raise Exception("API call failed")

        return data

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



