import argparse
import os
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
    if isinstance(dataset, datasets.DatasetDict):
        dataset = dataset["train"]

else:
    dataset = datasets.load_dataset(args.dataset, split="train")

print("Dataset loaded")
print(dataset)


def process(ex):
    main = ex["files"][ex["main"]]
    del ex["files"][ex["main"]]

    # add all the files before the main file
    buf = ""
    for _, txt in ex["files"].items():
        if txt is None:
            continue
        buf += txt + "\n"

    buf += main
    print(buf[:50].replace("\n", " "))
    return {"content": buf}


ds = dataset.map(process, remove_columns=dataset.column_names, num_proc=os.cpu_count())

print(ds)

if args.save_to_disk:
    ds.save_to_disk(args.save_to_disk)

if args.push:
    ds.push_to_hub(args.push)
