import datasets
import os
import datetime
from pathlib import Path
from tqdm import tqdm
import argparse

parser = argparse.ArgumentParser()
parser.add_argument("--papers", type=str, required=True)
parser.add_argument("--push", type=str, required=True)
args = parser.parse_args()

ds = datasets.load_dataset("CCRss/arXiv_dataset", split="train")


def process(ex):
    return ex


ds = ds.map(process, num_proc=os.cpu_count())
