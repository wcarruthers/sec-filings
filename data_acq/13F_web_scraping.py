from sec_edgar_downloader import Downloader
import pandas as pd
import os
#from bs4 import BeatifulSoup
#import datetime

def download_cik(filing, CIK):
    """
    Downloads files to machine in save_path
    filing = 13F-HR for quarterly filings
    CIK -  CIK number of fund of interest
    
    Data gets saved in data/sec-edgar-filings/{cik}/{filing}/
    
    """
    save_path = 'data'
    dl = Downloader(save_path)
    dl.get(filing, CIK)

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

def download_cik_files(filing, ciks):
    for c in ciks:
        download_cik(filing, c)

if __name__ == "__main__":
    cik_path = os.path.join('data', 'cik_file.txt')
    ciks = get_ciks(cik_path)
    download_cik_files('13F-HR', ciks)