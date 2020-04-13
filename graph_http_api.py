from requests                       import          post
from json                           import          dumps
from itertools.chain                import          from_iterable
from requests.auth                  import          HTTPBasicAuth

def tx_to_graph(data, 
    url="http://localhost:7474", 
    auth=HTTPBasicAuth("neo4j", "neo4j")):
    """commit a transaction or transactions to graph"""
    r = post(url=url + "/db/neo4j/tx/commit", 
            data=dumps(data),
            auth=auth,
            headers = {
                'Accept': 'application/json;charset=UTF-8',
                'Content-Type':'application/json'
            }
        )
    return r

def batch(data, nrows=100000):
    """divides pandas dataframe into batches of transactions"""
    for i in range(0, len(data), nrows):
        yield data[i:i+nrows].to_dict(orient="records")

def statement(cypher, properties={}, parameters={}):
    """formats a single cypher query with props into json dict"""
    return {
        "statements" : [ {
                "statement" : cypher,
                "parameters" : {
                "properties" : properties,
                **parameters}
            }  
        ]
    }

def statements(queries):
    """combines multiple json dict queries into a single one"""
    return {
        "statements" : 
            list(from_iterable(query["statements"] for query in queries
            )
        )
    } 