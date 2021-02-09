# Download Unpaywall

Script to download the PDFs of an Unpaywall snapshot, available [here](https://unpaywall.org/products/snapshot).

## Usage

```bash
pip3 install -r requirements.txt

python download_unpaywall.py --snapshot <extracted snapshot from link above> --dl_folder D:\\Data\pdfs
```

## Notes
* Will resume downloading based on the DOIs already tried in the created checkpoint file.
* Special care taken for arxiv to ensure `export.arxiv` is used.