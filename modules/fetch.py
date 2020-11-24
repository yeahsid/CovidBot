from database import api_call
import mysql.connector
import pylibmc
from dotenv import load_dotenv
import os
import sys
sys.path.append('modules/')

load_dotenv()
HOST = os.getenv("DBHOST")
PORT = os.getenv("DBPORT")
USER = os.getenv("DBUSER")
PASSWD = os.getenv("DBPASSWD")
DATABASE = os.getenv("DBDATABASE")


def get_country_stats(Country):

    try:
        client = pylibmc.Client(['127.0.0.1'])
        data = client.get(Country)
        print("Using Memcached")
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
                    sql = "SELECT * from country_stats WHERE Country = %s"
                    val = (Country, )
                    cursor.execute(sql, val)
                    result = cursor.fetchone()
                    print("Using DB call with Memcached")
                    if result is None:
                        print("Country not found")
                    else:
                        client.set(Country, result)
                        return result
                except:
                    print("Could not execute the SQL Statement")
            except:
                try:
                    print("Using API call with Memcached")
                    data = api_call()
                    for i in range(0, len(data["Countries"])):
                        Country_API = data["Countries"][i]["Country"]
                        if Country_API == Country:

                            result = data["Countries"][i]
                            client.set(Country, result)
                            return result
                        else:
                            print("Country not found")
                except:
                    print("API call failed")
        return data

    except Exception:
        try:
            print("Memcached is down , executing Database call directly ")
            covid19db = mysql.connector.connect(
                host=HOST,
                port=PORT,
                user=USER,
                passwd=PASSWD,
                database=DATABASE
            )
            try:
                cursor = covid19db.cursor(dictionary=True)
                sql = "SELECT * from country_stats WHERE Country = %s"
                val = (Country, )
                cursor.execute(sql, val)
                data = cursor.fetchone()
                if data is None:
                    print("Country not found")
                else:
                    return data
            except:
                print("SQL Execution failed", sys.exc_info()[1])
        except:
            try:
                print("Memcached and database are down . Using API directly")
                data1 = api_call()
                for i in range(0, len(data1["Countries"])):
                    Country_API = data1["Countries"][i]["Country"]
                    if Country_API == Country:

                        data = data1["Countries"][i]
                        return data

            except:
                print("All methods to fetch data have failed",
                      sys.exc_info()[1])


def get_stats():

    try:
        print("Using Memcached")
        client = pylibmc.Client(['127.0.0.1'])
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
                    sql = "SELECT * from global_stats "
                    cursor.execute(sql)
                    result = cursor.fetchone()
                    print("Using DB call with Memcached")
                    client.set("Global", result)
                    return result
                except:
                    print("Could not execute the SQL Statement")
            except:
                try:
                    print("Using API call with Memcached")
                    data = api_call()
                    return data
                except:
                    print("API call failed")

        return data

    except Exception:
        try:
            print("Memcached is down , executing Database call directly ",)
            covid19db = mysql.connector.connect(
                host=HOST,
                port=PORT,
                user=USER,
                passwd=PASSWD,
                database=DATABASE
            )
            try:
                cursor = covid19db.cursor(dictionary=True)
                sql = "SELECT * from global_stats"
                cursor.execute(sql)
                data = cursor.fetchone()
                return data
            except:
                print("SQL Execution failed", sys.exc_info()[1])
        except:
            try:
                print("Memcached and database are down . Using API directly")
                data = api_call()
                return data

            except:
                print("All methods to fetch data have failed",
                      sys.exc_info()[1])


print(get_stats())
