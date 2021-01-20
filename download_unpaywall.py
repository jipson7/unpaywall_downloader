import json
from queue import Queue
from concurrent.futures import ThreadPoolExecutor
from multiprocessing import cpu_count
import requests
import os
from tqdm import tqdm


class ThreadPoolExecutorWithQueueSizeLimit(ThreadPoolExecutor):
    def __init__(self, maxsize=100, *args, **kwargs):
        super(ThreadPoolExecutorWithQueueSizeLimit, self).__init__(*args, **kwargs)
        self._work_queue = Queue(maxsize=maxsize)

    def submit(self, fn, *args, **kwargs):
        if len(self._work_queue.queue) > 100:
            print(len(self._work_queue.queue))
        super(ThreadPoolExecutorWithQueueSizeLimit, self).submit(fn, *args, **kwargs)


METADATA_PATH = "F://Datasets/unpaywall/unpaywall_snapshot_2020-10-09T153852.jsonl"

PDF_SAVE_FOLDER = "F://Datasets/unpaywall/pdfs/"

CHECKPOINT_FILE = "F://Datasets/unpaywall/downloaded.txt"

os.makedirs(PDF_SAVE_FOLDER, exist_ok=True)

try:
    with open(CHECKPOINT_FILE) as cf:
        already_downloaded = set(cf.readlines())
except FileNotFoundError:
    already_downloaded = set()

thread_count = cpu_count() * 8

executor = ThreadPoolExecutorWithQueueSizeLimit(max_workers=thread_count, maxsize=thread_count)


def save_pdf(pdf_url, doi):
    response = requests.get(pdf_url, timeout=10, allow_redirects=True)
    pdf_filename = os.path.join(PDF_SAVE_FOLDER, doi + ".pdf")
    with open(pdf_filename, 'wb') as f:
        f.write(response.content)


with open(METADATA_PATH, encoding='utf-8') as mf:
    with open(CHECKPOINT_FILE, "a") as cf:
        for line in tqdm(mf, unit=" pdfs", desc="Downloading PDFs"):
            item = json.loads(line)
            doi = item["doi"].replace("/", "-")
            if doi in already_downloaded:
                continue
            cf.write(doi + "\n")
            if item["is_oa"]:
                pdf_url = item["best_oa_location"]["url_for_pdf"]
                if pdf_url:
                    if "arxiv.org" in pdf_url:
                        # Use export of arxiv so they dont block our IP
                        pdf_url = pdf_url.replace("arxiv.org", "export.arxiv.org")
                    executor.submit(save_pdf, pdf_url, doi)
