# %%
#imports
from sqlalchemy import inspect

import sqlalchemy
import os

# %%
def import_query(path):
    '''func to import query.sql file'''
    with open(path, 'r') as open_file:
        query = open_file.read()
    return query

def process_date(query, date_ingestion, engine):
    '''func that execute the query file based on a date of ingestions'''    
    # delete in case of duplicated ingestions
    delete = f'delete from tb_book_players where dtRef = "{date_ingestion}"'
    engine.execute(delete)

    query = query.format(date = date_ingestion)
    engine.execute(query)
# %%
# conecting python with db
engine = sqlalchemy.create_engine('sqlite:///../data/gc.db')

query = import_query('query.sql')

date = "2022-01-01"

process_date(query, date, engine)





# %%
