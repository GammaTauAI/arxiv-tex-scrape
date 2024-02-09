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

base_path = Path(args.papers)
TOO_LARGE = 10_000_000  # chars


ds2 = []
for ex in tqdm(ds, total=len(ds)):
    p_id = ex["id"]
    p_dir = base_path / p_id
    if p_dir.exists():
        # read all files in the directory
        files = {}
        skip = False
        for f in p_dir.iterdir():
            if skip:
                break
            if f.is_file():
                txt = f.read_text()
                if len(txt) > TOO_LARGE:
                    print(f"Skipping {f} as it is too large")
                    skip = True
                    continue
                files[f.name] = txt

        if len(files) > 10:
            print(f"Skipping {p_id} as it has too many files")
            continue
        ex2 = {"files": files, **ex}
        ds2.append(ex2)

ds = datasets.Dataset.from_list(ds2)
print(ds)

ds.push_to_hub(args.push)
