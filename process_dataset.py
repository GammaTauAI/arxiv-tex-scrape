import datasets
import os
import datetime
from pathlib import Path
from tqdm import tqdm
import argparse


def chunkify(lst, n):
    chunks = []
    for i in range(0, len(lst), n):
        chunk = []
        for j in range(n):
            if i + j < len(lst):
                chunk.append(lst[i + j])
        chunks.append(chunk)
    return chunks


parser = argparse.ArgumentParser()
parser.add_argument("--papers", type=str, required=True)
parser.add_argument("--push", type=str, required=True)
args = parser.parse_args()

ds = datasets.load_dataset("CCRss/arXiv_dataset", split="train")

base_path = Path(args.papers)
TOO_LARGE = 10_000_000  # chars


def find_main_file(files: dict):
    # the main file will have:
    # - \title
    # - \begin{abstract}, \end{abstract}
    # - \begin{document}, \end{document}

    # find candidate files
    candidates = []
    for f, txt in files.items():
        if "\\title" in txt and "\\begin{abstract}" in txt and "\\end{abstract}" in txt and "\\begin{document}" in txt and "\\end{document}" in txt:
            candidates.append(f)

    if len(candidates) == 0:
        return None

    # if there is only one candidate, return it
    if len(candidates) == 1:
        return candidates[0]

    # if there are multiple candidates, return the one with the most words
    best = None
    best_n = 0
    for c in candidates:
        n = len(files[c].split())
        if n > best_n:
            best = c
            best_n = n

    return best


ds2 = {
    "main": [],
    "files": [],
}
ex1 = ds[0]
for k in ex1.keys():
    ds2[k] = []

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

        main_file = find_main_file(files)
        if main_file is None:
            print(f"Skipping {p_id} as it has no main file")
            continue

        ex2 = {"main": main_file, "files": files, **ex}
        for k in ex2.keys():
            ds2[k].append(ex2[k])

ds = datasets.Dataset.from_dict(ds2)

ds.push_to_hub(args.push)
