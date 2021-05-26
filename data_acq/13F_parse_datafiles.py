import pandas as pd
import os
#from bs4 import BeatifulSoup
#ImportError: cannot import name 'BeatifulSoup' from 'bs4' (C:\Users\wcarr\Anaconda3\envs\py_env\lib\site-packages\bs4\__init__.py)
import bs4
import datetime
import pandas as pd

def parse_13f_file(file, CIK):
    try:
        xml = open(os.path.join('data', 'sec-edgar-filings', CIK, '13F-HR', file, 'full-submission.txt')).read()
        soup = bs4.BeautifulSoup(xml, 'lxml')
    except:
        print("failed to open", os.path.join('data', 'sec-edgar-filings', CIK, '13F-HR', file, 'full-submission.txt'))
        return
    
    try:
        date = datetime.datetime.strptime(soup.find('signaturedate').contents[0], '%m-%d-%Y')
        print(date)
        name = soup.find('name').contents[0]
    except:
        print("failed date")
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
            #print(d)
        df = pd.DataFrame(d)
        print(df.head())
        cols.append('date')
        cols.append('fund_cik')
        cols.append('fund')
        #print(df.columns)
        #print(cols)
        try:
            df.columns = cols
            print(df.head())
        except:
            print("COLUMN ERROR")
            return
        print(df.head())
        
        return df

def parse_cik_files(CIK, f):
    #files = os.listdir(os.path.join('data', 'sec-edgar-filings', CIK, '13F-HR'))
    files = []
    files.append(f)
    df = pd.DataFrame()
    for f in sorted(files):
        data = parse_13f_file(f, CIK)
        print(data)
        df.append(data)
        print(df)
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

if __name__ == "__main__":
    #cik_path = os.path.join('data', 'cik_file.txt')
    #ciks = get_ciks(cik_path)
    #df = pd.DataFrame()
    #for c in ciks:
     #   df_c = parse_cik_files(c)
     #   print(df_c.head())
     #   df = df.append(df_c)
    #print(df.head())
    #df['value'] = df['value'].astype(float)
    #print(df.groupby(['fund','date'])['value'].sum())
    parse_cik_files('0000089014', '0001567619-21-001894')
