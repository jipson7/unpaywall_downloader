import json
from multiprocessing import Pool, cpu_count
import requests
import argparse
import os
from tqdm import tqdm
import webdataset as wds

pdfs_per_shard = 1000  # TODO increase this


def chunks(lst, n):
    """Yield successive n-sized chunks from lst."""
    for i in range(0, len(lst), n):
        yield lst[i:i + n]


def download(args):
    os.makedirs(args.dl_folder, exist_ok=True)

    def download_chunk(chunk, shard_name):
        sink = wds.TarWriter(shard_name)
        for line in tqdm(chunk, unit=" pdfs", desc=f"Downloading PDFs to {shard_name}"):
            item = json.loads(line)
            doi = item["doi"]
            if item["is_oa"]:
                pdf_url = item["best_oa_location"]["url_for_pdf"]
                if pdf_url:
                    if "arxiv.org" in pdf_url:
                        # Use export of arxiv so they dont block our IP
                        pdf_url = pdf_url.replace("arxiv.org", "export.arxiv.org")
                    response = requests.get(pdf_url, timeout=10, allow_redirects=True)
                    if response.ok:
                        sink.write({
                            "__key__": doi,
                            "pdf": response,
                            "json": item
                        })

    with Pool(cpu_count()) as pool:
        with open(args.snapshot, encoding='utf-8') as mf:
            chunk = []
            chunk_idx = 0
            for line in mf:
                chunk.append(line)
                if len(chunk) == pdfs_per_shard:
                    pool.apply_async(download_chunk, args=(chunk, os.path.join(args.dl_folder, f"{chunk_idx:06}.tar")))
                    chunk = []
                    chunk_idx += 1
            pool.close()
            pool.join()



if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Download PDFs from snapshot')
    parser.add_argument('--snapshot', help='Unpaywall snapshot file', type=str, required=True)
    parser.add_argument('--dl_folder', help='Path to download files to', type=str, default="./data")
    download(parser.parse_args())