import requests
import pymysql
import sqlite3
import pandas as pd


def get_rates_from_api(start_date, end_date):
    url = f'https://api.exchangerate.host/timeseries?start_date={start_date}&end_date={end_date}'
    response = requests.get(url)
    print(response)
    data = response.json()

    print(data["rates"])

    df_rates_by_date = pd.DataFrame(data["rates"])
    df_rates_by_date.index.name = 'currency'
    df_rates_by_date.reset_index(inplace=True)

    print(df_rates_by_date)

    return df_rates_by_date


def connect_database():
    connection = sqlite3.connect('rate_database')
    c = connection.cursor()

    c.execute('''
              CREATE TABLE IF NOT EXISTS exchange_rate
              ([currency] TEXT)
              ''')

    connection.commit()

    return connection


def add_to_database(connection, dataframe_curr_rate_by_date):
    c = connection.cursor()

    dataframe_curr_rate_by_date.to_sql('exchange_rate', connection, if_exists='replace', index=False)

    c.execute('''  
              SELECT * FROM exchange_rate
              ''')

    for row in c.fetchall():
        print(row)


# Press the green button in the gutter to run the script.
if __name__ == '__main__':
    start = '2022-06-19'
    end = '2022-07-01'
    df1 = get_rates_from_api(start, end)

    conn = connect_database()

    add_to_database(conn, df1)

    conn.close()
