import pyad.adquery

def ad_query(base_dn, attributes, where_clause):

    if isinstance(base_dn, str) == False:
        raise Exception('The basedn parameter must be a string')

    if isinstance(attributes, list) == False:
        raise Exception('The attributes parameter must be a list')

    if isinstance(where_clause, str) == False:
        raise Exception('The where parameter must be a string (in double quotes)')

    #Connect to Active Directory
    q = pyad.adquery.ADQuery()

    #Execute the AD Query
    q.execute_query(where_clause = where_clause,
                    attributes = attributes,
                    base_dn = base_dn)

    #Iterate over the results and return a list
    results = [r for r in q.get_results()]

    return results
