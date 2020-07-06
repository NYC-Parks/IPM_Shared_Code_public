import pandas as pd
import numpy as np
import sqlalchemy
import urllib
import hashlib

#Create a function for generating row sha256 hashes by concatenating all of the column values
def hash_rows(df, exclude_cols, hash_name):
    #Get all of the column names from the input dataframe
    col_names = df.columns.values
    #Always add the name of hash_column to the list of columns in order to avoid recursive hashing
    exclude_cols.append(hash_name)

    #If the row_hash column does not exist then create it and fill it with NaNs
    if hash_name not in col_names:
        df[hash_name] = np.nan

    #The first element is the index, the second element of the iterrows tuple is the data
    for i, r in df.iterrows():
        row = r
        row_str = ''
        for c in col_names:
            #Exclude rows we don't want in the hash
            if c not in exclude_cols:
                #Concatenate row value converted to a string and stripped
                row_str = row_str + str(row[c]).strip()

        #Digest the hash so values can be used in comparison
        df.iloc[i, df.columns.get_loc(hash_name)] = hashlib.sha256(row_str.encode()).hexdigest()

def dml_verb(row, hash_name, suffix):

    hash_name_old = hash_name + suffix
    null_suffix = '_null'
    hash_null = hash_name + null_suffix
    hash_old_null = hash_name_old + null_suffix

    ## If the row hashes are equal set the dml_verb to None
    if row[hash_name] == row[hash_name_old]:
        dml_verb = None
    ## If the old dataframe has None as the row hash value set the dml_verb to I for Insert
    elif row[hash_old_null] == True:
        dml_verb = 'I'
    ## If the new dataframe has None as the row hash value set the dml_verb to D for Delete
    elif row[hash_null] == True:
        dml_verb = 'D'
    ## If the row hashes are not equal set the dml_verb to U for Update
    elif ((row[hash_name] != row[hash_name_old]) and
         (row[hash_old_null] == False) and
         (row[hash_null] == False)):
        dml_verb = 'U'

    return dml_verb

def check_deltas(new_df, old_df, on, hash_name, dml_col):

    left_suffix = ''
    right_suffix = '_old'
    hash_name_old = hash_name + right_suffix

    null_suffix = '_null'
    hash_null = hash_name + null_suffix
    hash_old_null = hash_name_old + null_suffix

    #The new dataframe should always be on the left and never be given a suffix
    delta_df = (new_df.merge(old_df, how = 'outer', on = on, suffixes = (left_suffix, right_suffix))
               .fillna(value = np.nan, axis = 1))

    delta_df[hash_null] = delta_df[hash_name].isnull()
    delta_df[hash_old_null] = delta_df[hash_name_old].isnull()

    #Set the value of the column that holds the DML verb (insert, update, delete)
    delta_df[dml_col] = delta_df.apply(lambda row: dml_verb(row, hash_name, right_suffix), axis = 1)

    delta_df = delta_df.drop(columns = [hash_null, hash_old_null])

    return delta_df
