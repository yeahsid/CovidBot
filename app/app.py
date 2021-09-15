from functions import getStats, getCountryStats
from ariadne import QueryType, make_executable_schema
from ariadne.asgi import GraphQL
from ariadne import gql
from starlette.applications import Starlette
from starlette.middleware import Middleware
from starlette.middleware.cors import CORSMiddleware
from starlette.routing import Mount
from starlette.staticfiles import StaticFiles


# gunicorn -w 3 -k uvicorn.workers.UvicornWorker app:app -b 0.0.0.0:8000
# pkill gunicorn
# Define types using Schema Definition Language (https://graphql.org/learn/schema/)
# Wrapping string in gql function provides validation and better error traceback
type_defs = gql("""
    type Query {
        countryStats(slug: String!): countryStats
        globalStats: globalStats
    }

    type countryStats {
        country: String
        countryCode: String
        slug: String
        dailyNewConfirmed: Int
        totalConfirmed: Int
        dailyNewDeaths: Int
        dailyNewRecovered: Int
        totalDeaths: Int
        totalRecovered: Int
        dateUpdatedDate: String
    }

    type globalStats {
        dailyNewConfirmed: Int
        totalConfirmed: Int
        dailyNewDeaths: Int
        dailyNewRecovered: Int
        totalDeaths: Int
        totalRecovered: Int
    }


""")

# Map resolver functions to Query fields using QueryType
query = QueryType()


@query.field("countryStats")
def resolveCountryStats(*_, slug):

    call = getCountryStats(slug)
    return call


@query.field("globalStats")
def resolveGlobalStats(*_):

    call = getStats()
    return call


# Create executable GraphQL schema
schema = make_executable_schema(type_defs, query)

middleware = [
    Middleware(CORSMiddleware, allow_origins=['*'], allow_methods=['POST'])
]
# Create an ASGI app using the schema

routes = [

    Mount("/graphql", GraphQL(schema, debug=False)),
    Mount('/', app=StaticFiles(directory='static', html=True), name="static"),


]
#app.mount("/", GraphQL(schema, debug=True , introspection = True))
app = Starlette(routes=routes)
