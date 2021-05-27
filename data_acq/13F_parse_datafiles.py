import pandas as pd
import os
#from bs4 import BeatifulSoup
#ImportError: cannot import name 'BeatifulSoup' from 'bs4' (C:\Users\wcarr\Anaconda3\envs\py_env\lib\site-packages\bs4\__init__.py)
import bs4
import datetime
import pandas as pd
from sqlalchemy import create_engine

def parse_13f_file(file, CIK):
    try:
        xml = open(os.path.join('data', 'sec-edgar-filings', CIK, '13F-HR', file, 'full-submission.txt')).read()
        soup = bs4.BeautifulSoup(xml, 'lxml')
    except:
        print("failed to open", os.path.join('data', 'sec-edgar-filings', CIK, '13F-HR', file, 'full-submission.txt'))
        return
    
    try:
        date = datetime.datetime.strptime(soup.find('signaturedate').contents[0], '%m-%d-%Y')
        #print(date)
        name = soup.find('name').contents[0]
        #print(name)
    except:
        #old files are not in the xml as expected
        #print(file)
        #print("failed date")
        return
        
    cols = ['nameOfIssuer', 'cusip', 'value', 'sshPrnamt', 'sshPrnamtType', 'putCall']
    
    d = []
    
    for infotable in soup.find_all(['ns1:infotable', 'infotable']):
        row = []
        for col in cols:
            
            data = infotable.find([col.lower(), 'ns1:' + col.lower()])
            
            if data is not None:
                #print(data)
                row.append(data.text.strip())
            else:
                row.append('NaN')
        row.append(date)
        row.append(CIK)
        row.append(name)
        d.append(row)
            
    df = pd.DataFrame(d)
    cols.append('date')
    cols.append('fund_cik')
    cols.append('fund')
        #print(df.columns)
        #print(cols)
    try:
        df.columns = cols
        return df
    except:
        print("COLUMN ERROR")
        return


def parse_cik_files(CIK):
    files = os.listdir(os.path.join('data', 'sec-edgar-filings', CIK, '13F-HR'))
    df = pd.DataFrame()
    for f in sorted(files):
        data = parse_13f_file(f, CIK)
        df = df.append(data)
    return df

def get_ciks(cik_path):
    """
    Imports a text tab-delimited files
    with the CIKs you are interested in
    e.g.
    CIK
    123
    234
    
    """
    df = pd.read_csv(cik_path, sep = "\t", dtype={'CIK': object})
    return df.CIK.values

def save_to_db(df):
    try:
        os.mkdir(os.path.join('data', 'sec-edgar-filings', 'sec_db'))
    except OSError as error: 
        print(error)
    engine = create_engine('sqlite:///data/sec-edgar-filings/sec_db/SEC.db', echo = True)
    #engine = create_engine('sqlite:///SEC.db', echo=True)
    sqlite_connection = engine.connect()
    sqlite_table = "13F_db"
    df.to_sql(sqlite_table, sqlite_connection, if_exists='replace')
    sqlite_connection.close()

if __name__ == "__main__":
    cik_path = os.path.join('data', 'cik_file.txt')
    ciks = get_ciks(cik_path)
    df = pd.DataFrame()
    for c in ciks:
        df_c = parse_cik_files(c)
        df = df.append(df_c)
    df['value'] = df['value'].astype(float)
    print(df.groupby(['fund','date'])['value'].sum())
    save_to_db(df)
    
    #for testing
    #df = parse_cik_files('0000089014', '0001567619-21-001894')
    #print(df.head())
    #df['value'] = df['value'].astype(float)
    #print(df.groupby(['fund','date'])['value'].sum())
