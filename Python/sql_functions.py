import sqlalchemy
import pandas as pd
import urllib
import numpy as np

#This function provides the ability to update SQL updates from pandas dataframes using the SQLAlchemy API
def sql_update(df, sql_table, engine, where_col, exclude_cols = None):

    #Check if the primary key (where_col) parameter is a single column (string) or a multi-column
    #composite key (list). Set the suffixes of the keys for parameter mapping.
    where_suffix = '__df'

    #If there is a single where column, simply add the where suffix to the name of the where column.
    if isinstance(where_col, str):
        where_col_new = where_col + where_suffix
        composite_where = False
        #Create the dictionary to rename the primary key column(s)
        where_rnm_dict = {where_col: where_col_new}

    #If there are multiple where columns, add the where suffixes to each of the where columns.
    elif isinstance(where_col, list):
        where_col_new = [c + where_suffix for c in where_col]
        composite_where = True
        #Create the dictionary to rename the primary key column(s)
        where_rnm_dict = {old: new for old,new in zip(where_col, where_col_new)}

    #If this parameter is not a string or list then raise an Exception
    else:
        raise Exception('The where_col parameter must be a string or list')

    #Connect to the engine
    con = engine.connect()

    #Create the Metadata object
    meta = sqlalchemy.MetaData(con)

    #Reflect the schema of the sql_table to the meta object
    meta.reflect(only = [sql_table], views = True)

    #Connect to the table object
    tbl = sqlalchemy.Table(sql_table, meta)

    #Get the where column objects
    sa_where_col = [c for c in tbl.columns if c.name in where_col]

    #If columns are not being removed, then simply rename the where column
    if exclude_cols == None:
        df = (df.copy()
              #NaN will not be translated correctly and cause a datatype error, None is required
              .replace({pd.np.nan: None})
              #Rename the where columns
              .rename(columns = where_rnm_dict))
    else:
          df = (df.copy()
               #Drop the columns specified by the input parameter, do this if you want to prevent certain columns from updating
               .drop(columns = exclude_cols)
               #NaN will not be translated correctly and cause a datatype error, None is required
               .replace({pd.np.nan: None})
               .rename(columns = where_rnm_dict))

    #If the dataframe has rows then do the update
    if df.shape[0] > 0:
        #Create the list of dictionaries from the dataframe
        values = [dict(v) for i, v in df.iterrows()]

        #If there is more than one where column exclude these columns from the values parameter binding
        if composite_where == True:
            #Create the value parameters, exclude the where columns by joining the list of these columns with original and new names
            param_dict = {c: sqlalchemy.bindparam(c) for c in df.columns.values
                          if c not in where_col + where_col_new}
        else:
            #Create the value parameters, exclude the where columns with original and new names
            param_dict = {c: sqlalchemy.bindparam(c) for c in df.columns.values
                          if c not in [where_col, where_col_new]}

        #Create the SQL DML object
        #For single where column updates this is easy, it's where followed by values
        if composite_where == False:
            sql = tbl.update().where(sa_where_col[0] == sqlalchemy.bindparam(where_col_new)).values()

        #If issues are ever encountered, multiple where columns may need to use the and_() method
        #Invoke multiple where clauses to create an and. This is more complicated because you need an update followed by multiple where columns.
        #After trial and error I chose to add multiple where columns because the SQLAlchemy documentation says it should work "in most cases"
        else:
            sql = tbl.update()
            for s, d in zip(sa_where_col, where_col_new):
                    sql = sql.where(s == sqlalchemy.bindparam(d))
            sql = sql.values()

        #Set the parameters for the values() function/method with settatr
        setattr(sql, 'parameters', param_dict)

        #Execute the SQL DML object
        con.execute(sql, values)

        con.close()
        #Should probably consider returning a message of success with a row count
        #return([param_dict, values])

#This function provides the ability to insert into SQL from pandas dataframes using the SQLAlchemy API, while eventually providing a sometimes needed alternative to to_sql
def sql_insert(df, sql_table, engine, exclude_cols):
    #Connect to the engine
    con = engine.connect()
    
    #Create the Metadata object
    meta = sqlalchemy.MetaData(con)
    
    #Reflect the schema of the sql_table to the meta object
    meta.reflect(only = [sql_table], views = True)
    
    #Connect to the table object
    tbl = sqlalchemy.Table(sql_table, meta)
    
    if exclude_cols == None:
        df = (df.copy()
              .replace({pd.np.nan: sqlalchemy.null()}))
    else: 
          df = (df.copy()
          .drop(columns = exclude_cols)
          .replace({pd.np.nan: sqlalchemy.null()}))
    
    values = [dict(v) for i, v in df.iterrows()]
    #Create the SQL DML object
    sql = tbl.insert().values(values)
    
    #Excute the SQL DML object
    con.execute(sql)
    
    #con.commit()
    con.close()