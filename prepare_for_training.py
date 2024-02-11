import argparse
import datasets
from tqdm import tqdm
import os

parser = argparse.ArgumentParser()
parser.add_argument("--dataset", type=str, required=True)
parser.add_argument("--push", type=str, required=False)
parser.add_argument("--save-to-disk", type=str, required=False)
args = parser.parse_args()

if os.path.exists(args.dataset):
    dataset = datasets.load_from_disk(args.dataset)
else:
    dataset = datasets.load_dataset(args.dataset)

dataset = dataset["train"]


