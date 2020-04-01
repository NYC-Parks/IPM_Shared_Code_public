import binascii

from pandas import read_sql
import shapely.wkb

from geopandas import GeoSeries, GeoDataFrame
import re

#This function is a modification of geopandas.read_postgis() function
def read_mssql(sql, #SQL Statement used to pull data
                con, #pyodbc database connection
                geom_raw = 'Shape', #The raw (original geometry column) that is going to be converted and dropped
                geom_col = 'geom', #The name of the WKB converted geometry column
                crs=None, #The Projection to define for the geodataframe
                print_sql = True, #Print the sql statement in case you want to debug it in SQL server
                index_col=None,
                coerce_float=True, 
                params=None):

    """
    reads table, including geometry, from the parks MS SQL database and outputs to Geopandas GeoDataFrame

    Example:
    TEST!
    con = pyodbc.connect(...)
    sql = 'select * from parksgis.dpr.property_evw'
    parks = read_mssql(sql, con, geom_raw = 'Shape', geom_col = 'geo', print_sql = True)

    Parameters
    ----------
    sql : string,
        SQL string used to pull data
    con : pyodbc.Connection,
        database connection object
    geom_raw : BLOB, 
        The raw (original geometry column) that is going to be converted and dropped
    crs : dict, 
        The Projection to define for the geodataframe
    print_sql : Boolean,
        Prints the sql statement in case you want to debug it in SQL server, if True
    index_col :

    coerce_float :

    params : 



    Returns
    -------
    GeoDataFrame (containing a geometry column) corresponding to the result of the query string.
   
    """

    #Find the from (case insensitive) statements in the sql statement, find the "from" at word boundaries only!
    pat = re.compile(r'\bfrom\b', re.I)
    #Find each match of the from statement
    from_match = re.finditer(pat, sql) 
    #Create an empty list to store the from statement matches 
    matches = []
    #Raise an error if there are no regex matches to "From".
    if from_match is None:
        raise ValueError('Your query is missing a "FROM" clause!')
    #Iterate through the regular expression matches appending the position to the 
    for i in from_match:
        matches.append(i.start())
        
    print('Note: There were ' + str(len(matches)) + ''' "FROM" clauses or variables containing the word "FROM" in your query.
           The geometry conversion was added to the outermost "SELECT" clause. \n''')
    
    #Find the minimum (outer most) "from" statement to append geometry conversion type
    #Because the regular expression operates on word boundaries, 1 needs to be subtracted from the minimum to include
    #the space before the "from."
    st_start = min(matches) - 1
    
    sql_nocount = 'set nocount on; '
    #Drop the raw SQL geometry column which is probably encoded as a Binary Large OBject (BLOB).
    sql_alter = ' alter table #temp drop column ' + geom_raw
    #Select the data from the temporary table
    sql_pull = ' select * from #temp'
    sql_drop = ' drop table #temp'
    #Concatenate the pieces that make up the first half of the SQL query: the set nocount option, the outermost select, 
    #the geometry conversion and the temporary table creation.
    sql_stmt1 = sql_nocount + sql[0: st_start] + ', ' + geom_raw + '.STAsBinary() as ' + geom_col + ' into #temp ' 
    #Concatenate the pieces that make up the second half of the SQL query: the first from an forward, the alter table statment, 
    #the selection of data from the temporary table and the drop table statement.
    sql_stmt2 = sql[st_start:len(sql)] + sql_alter + sql_pull + sql_drop
    
    #Check to see if the geom_raw column is None, if so raise and error.
    #Generate the new SQL statement with the conversion of the geometry/geography to the WKB representation.
    sql2 = sql_stmt1 + sql_stmt2
    
    #Print the SQL statement in case you want to debug it in sql server.
    if print_sql is True:
        print ('The SQL Statement generated and sent to execute was:\n' + sql_nocount + '\n' + sql[0: st_start] + ',' +  
                geom_raw + '.STAsBinary() as ' + geom_col + '\n' + ' into #temp ' + '\n' + sql[st_start:len(sql)] + '\n' +
                sql_alter + '\n' + sql_pull + '\n' + sql_drop + '\n')

    #Execute the SQL statement and read the data into a pandas dataframe object.
    df = read_sql(sql2, con, index_col=index_col, coerce_float=coerce_float, params=params)

    wkb_geoms = df[geom_col]
    
    #Interpret the WKB representation into something that geopandas understands
    #http://gis.stackexchange.com/questions/19854/what-is-the-format-of-geometry-data-type-of-sqlserver-2008
    #shapely.wkb.loads loads a geometry from a WKB byte string, or hex-encoded string if hex=True.
    #binascii.hexlify returns the hexadecimal representation of the binary data. Every byte of data is converted into the 
    #corresponding 2-digit hex representation. The returned bytes object is therefore twice as long as the length of data.
    s = wkb_geoms.apply(lambda x: shapely.wkb.loads(binascii.hexlify(x), hex = True))

    df[geom_col] = GeoSeries(s)
    
    #Define the projection as New York Long Island (ftUS) since all Parks data is in this projection.
    #http://www.spatialreference.org/ref/epsg/2263/
    if crs is None:
        crs = {'init' :'epsg:2263'}
        print ('Note: No Coordinate Reference System (CRS) was specified! The CRS was set to ' + crs.values()[0] + 
              ', New York Long Island (ftUS).')
        
    #Return the GeoDataFrame
    return GeoDataFrame(df, crs=crs, geometry=geom_col)




def read_geosql(sql, #SQL Statement used to pull data
                con, #pyodbc database connection
                geom_raw = 'Shapes', #The raw (original geometry column)
                geom_col = 'geom', #The name of the WKB converted geometry column
                crs=None, #The Projection to define for the geodataframe
                index_col=None,
                coerce_float=True, 
                params=None):
    """
    reads county geometry from the parks MS SQL database

    Parameters
    ----------
    sql : string
        SQL string used to pull data
    con : pyodbc Connection
        pyodbc database connection
    geom_raw : string, optional
        The raw (original geometry column)
    (rest of arguments)

    Returns
    -------
    geom : list of tuples
        county geometries
   
    """

    #Find the from (case insensitive) statements in the sql statement, find the "from" at word boundaries only!
    pat = re.compile(r'\bfrom\b', re.I)
    #Find each match of the from statement
    from_match = re.finditer(pat, sql) 
    #Create an empty list to store the from statement matches 
    matches = []
    #Iterate through the regular expression matches appending the position to the 
    for i in from_match:
        matches.append(i.start())
    
    #Find the minimum (outer most) "from" statement to append geometry conversion type
    #Because the regular expression operates on word boundaries, 1 needs to be subtracted from the minimum to include
    #the space before the "from."
    st_start = min(matches) - 1
    
    #Generate the new sql statement with the conversion of the geometry/geography to the WKB representation.
    sql2 = sql[0: st_start] + ',' + geom_raw + '.STAsBinary() as ' + geom_col + sql[st_start:len(sql)]
    
    #Execute the SQL statement.
    df = read_sql(sql2, con, index_col=index_col, coerce_float=coerce_float,
                  params=params)
    
    #Drop the raw SDE hex geometry column because ESRI is the worst
    try:
        df = df.drop(geom_raw, axis = 1)
    except ValueError:
        raise ValueError("geom_raw column {} was invalid - please supply a valid geom_raw column name ".format(geom_raw))

    if geom_col not in df:
        raise ValueError("Query missing geometry column '{0}'".format(
            geom_col))

    wkb_geoms = df[geom_col]
    #Interpret the WKB representation into something that geopandas understands
    s = wkb_geoms.apply(lambda x: shapely.wkb.loads(binascii.unhexlify(binascii.hexlify(x).decode())))

    df[geom_col] = GeoSeries(s)
    
    #Define the projection as New York Long Island (ftUS) since all Parks data is in this projection.
    #http://www.spatialreference.org/ref/epsg/2263/
    if crs is None:
        crs = {'init' :'epsg:2263'}

    return GeoDataFrame(df, crs=crs, geometry=geom_col)