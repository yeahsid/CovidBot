from modules.fetch import getStats, getCountryStats
from ariadne.asgi import GraphQL
from ariadne import ObjectType, QueryType, gql, make_executable_schema
from modules.fetch import apiCall


# Define types using Schema Definition Language (https://graphql.org/learn/schema/)
# Wrapping string in gql function provides validation and better error traceback
type_defs = gql("""
    type Query {
        countryStats(country: String!): countryStats
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
def resolveCountryStats(*_, country):

    call = getCountryStats(country)
    return call


@query.field("globalStats")
def resolveGlobalStats(*_):

    call = getStats()
    return call


# Create executable GraphQL schema
schema = make_executable_schema(type_defs, query)

# Create an ASGI app using the schema, running in debug mode
app = GraphQL(schema, debug=True)
