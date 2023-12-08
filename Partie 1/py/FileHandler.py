import pandas as pd
from requests import get
from gzip import open as gopen
from shutil import copyfileobj
import os

def setup_temp(root="."):
    dtype={
    'tconst': 'string',
    'titleType': 'string',
    'primaryTitle': 'string',
    'originalTitle': 'string',
    'isAdult': 'string',
    'startYear': 'string',
    'endYear': 'string',
    'runtimeMinutes': 'string'
    }
    file = pd.read_csv(root+"/tsv/basics.tsv", sep="\t", dtype=dtype).iloc[:,:2]
    file = file.loc[file.iloc[:,1] == 'movie']
    file = pd.merge(file, pd.read_csv(root+"/tsv/ratings.tsv", sep="\t"), on="tconst")
    file.to_csv(root+"/outputs/temp.txt", sep="\t", header=False, index=False)

def get_basics_tsv(root="."):
    open(root+"/outputs/basics.gz", "wb").write(get("https://datasets.imdbws.com/title.basics.tsv.gz").content)
    with gopen(root+"/outputs/basics.gz", 'rb') as f_in:
        with open(root+"/tsv/basics.tsv", 'wb') as f_out:
            copyfileobj(f_in, f_out)

def cleanup(root="."):
    if os.path.exists(root+"/outputs/basics.gz"):
        os.remove(root+"/outputs/basics.gz")

    if os.path.exists(root+"/outputs/temp.txt"):
        os.remove(root+"/outputs/temp.txt")

    for i in range(1,4):
        if os.path.exists(root+"/outputs/Q"+str(i)+"/part-00000"): 
            if os.path.exists(root+"/outputs/Q"+str(i)+".txt"):
                os.remove(root+"/outputs/Q"+str(i)+".txt")
            os.rename(root+"/outputs/Q"+str(i)+"/part-00000", root+"/outputs/Q"+str(i)+".txt")
            try: os.rmdir(root+"/outputs/Q"+str(i))
            except: pass
            
