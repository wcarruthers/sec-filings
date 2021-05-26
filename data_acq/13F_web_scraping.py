from sec_edgar_downloader import Downloader
import pandas as pd
#import os
from bs4 import BeatifulSoup
#import datetime

def download_files(filing, CIK):
    """
    Downloads files to machine in save_path
    filing = 13F-HR for quarterly filings
    CIK -  CIK number of fund of interest
    """
    save_path = 'data'
    dl = Downloader(save_path)
    dl.get(filing, CIK)

