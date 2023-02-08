# %%
#imports
from sqlalchemy import Table, MetaData
from tqdm       import tqdm

import sqlalchemy 
import datetime
import os

# %%

def dates_to_list(dt_start, dt_end):
    '''func that return a list of days based on start date and end date'''
    start_date = datetime.datetime.strptime(dt_start, '%Y-%m-%d')
    end_date   = datetime.datetime.strptime(dt_end, '%Y-%m-%d')

    days_diff = (end_date - start_date).days
    lst_days = [(start_date + datetime.timedelta(i)).strftime('%Y-%m-%d') for i in range(days_diff+1)]

    return lst_days

def backfill(query, engine, dt_start, dt_end):
    '''func that will run process date function for each day of a list of days'''
    dates = dates_to_list(dt_start, dt_end)
    for d in tqdm(dates):
        process_date(query, d, engine)

def import_query(path):
    '''func to import query.sql file'''
    with open(path, 'r') as open_file:
        query = open_file.read()
    return query



def process_date(query, date_ingestion, engine):
    '''func that execute the query file based on a date of ingestions'''
    conn = engine.connect()

    # delete in case of duplicated ingestions
    #delete = tb_book_players.delete().where(tb_book_players.c.dtRef == date_ingestion)
    #conn.execute(delete)

    query = query.format(date=date_ingestion)
    conn.execute(query)

    conn.close()
 
# %%
# conecting python with db
engine = sqlalchemy.create_engine('sqlite:///../data/gc.db')

query = import_query('query.sql')

dt_start = input('Enter a start date: ')
dt_end   = input('Enter a end date: ')

backfill(query, engine, dt_start, dt_end)
