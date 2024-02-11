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


content = []
for ex in tqdm(dataset):
    main = ex["files"][ex["main"]]
    del ex["files"][ex["main"]]

    # add all the files before the main file
    buf = ""
    for f, txt in ex["files"].items():
        buf += txt + "\n"

    buf += main
    content.append(buf)

ds = dataset.add_column("content", content)
if args.save_to_disk:
    ds.save_to_disk(args.save_to_disk)

if args.push:
    ds.push_to_hub(args.push)
