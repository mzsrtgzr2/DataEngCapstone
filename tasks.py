from invoke import task
import requests
from typing import Sequence, Optional
import os
import pathlib

currentdir = pathlib.Path.cwd()

def _download_csv_to(src: str, dest: str):
    if os.path.isfile(dest):
        print('skipped {} already downloaded (to {})'.format(src, dest))
        return
    print('writing from {} to {}'.format(src, dest))
    req = requests.get(src)
    url_content = req.content
    pathlib.Path(dest.parent.absolute()).mkdir(parents=True, exist_ok=True)
    with open(dest, 'wb') as out:
        out.write(url_content)
    
@task
def download_raw_data(ctx):
    # activity download
    _download_csv_to(
        'https://query.data.world/s/nlzvfwgrwtkdbpmgzrpzwtyns44sjq',
        currentdir / 'out' / 'activity.csv'
    )

    _download_csv_to(
        'https://query.data.world/s/n2fh33k3646tkphijywr3tw5eeppyk',
        currentdir / 'out' / 'population.csv'
    )

    _download_csv_to(
        'https://raw.githubusercontent.com/'
        'govex/COVID-19/master/data_tables/vaccine_data/global_data/'
        'time_series_covid19_vaccine_global.csv',
        currentdir / 'out' / 'vaccinations.csv'
    )


    
    


