from invoke import task
from typing import Sequence, Optional
import inspect
import os
from os.path import join
currentdir = os.path.dirname(os.path.abspath(inspect.getfile(inspect.currentframe())))


@task
def download_raw_data(ctx):
    



