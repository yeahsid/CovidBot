from ariadne import ObjectType, QueryType, gql, make_executable_schema
from ariadne.asgi import GraphQL
import sys
sys.path.append('modules/')
import functions 

# Define types using Schema Definition Language (https://graphql.org/learn/schema/)
# Wrapping string in gql function provides validation and better error traceback
type_defs = gql("""
    type Query {
        stats(country: String!): [stats!]!
    }

    type stats {
        Country: String!
        CountryCode: String!
        Slug: String!
        NewConfirmed: Int
        TotalConfirmed: Int!
        NewDeaths: Int
        NewRecovered: Int
        TotalDeaths: Int!
        TotalRecovered: Int!
        Date: String!
    }
""")

# Map resolver functions to Query fields using QueryType
query = QueryType()


# Create executable GraphQL schema
schema = make_executable_schema(type_defs, query)

# Create an ASGI app using the schema, running in debug mode
app = GraphQL(schema, debug=True)

data = functions.api_call()
