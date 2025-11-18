
import os
import json
import requests
import warnings
from datetime import datetime
import logging
import anndata as ad
import boto3
import time
from tqdm import tqdm 
import psutil
import gc
import anndata as ad
from s3_index_utils import (
    load_s3_index_to_scfind,
    upload_index_to_s3_with_tags,
    save_index_metadata_json,
    save_all_index_versions_metadata_to_s3
)
from scfind import SCFind 

SEARCH_API_URL = "https://search.api.hubmapconsortium.org/v3/search"
ASSETS_API_URL = "https://assets.hubmapconsortium.org/"
BUCKET_NAME = "scfind-dataset"
S3_PREFIX = "prod_index"
feature_name = 'hugo_symbol'
cell_type_label = 'predicted_label'
clid_label = 'predicted_CLID'

timestamp = datetime.today().strftime('%Y-%m-%d')
index_new_path_celltype = f'cell_type_index_{timestamp}.bin'
index_new_path_dataset = f'dataset_index_{timestamp}.bin'

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

index_ct, ct_version_id = load_s3_index_to_scfind(BUCKET_NAME, f"{S3_PREFIX}/cell_type/index.bin")
index_dataset, ds_version_id = load_s3_index_to_scfind(BUCKET_NAME, f"{S3_PREFIX}/dataset/index.bin")

first_dataset_ct = True
first_dataset_ds = True

process = psutil.Process(os.getpid())

def log_memory_usage(stage: str):
    mem = process.memory_info().rss / (1024 ** 2)  # in MB
    logging.info(f"[MEMORY] {stage}: {mem:.2f} MB")

def fetch_datasets_azimuth(last_ts, last_uuid, search_after=None):
    query = {
        "query": {
            "bool": {
                "filter": [
                    {"term": {"calculated_metadata.annotation_tools.keyword": "Azimuth"}},
                    {"bool": {"should": [{"exists": {"field": "calculated_metadata"}}]}},
                    {"bool": {"must_not": {"exists": {"field": "next_revision_uuid"}}}},
                    {"range": {"published_timestamp": {"gt": last_ts}}}  # loose filter for pagination
                ]
            }
        },
        "size": 300,
        "_source": ["uuid", "published_timestamp"],
        "sort": ["published_timestamp", "uuid.keyword"]
    }

    if search_after:
        query["search_after"] = search_after

    logging.info("Query: %s", json.dumps(query, indent=2))
    response = requests.post(SEARCH_API_URL, json=query)
    response.raise_for_status()
    return response.json()

def get_last_run_state():
    if os.path.exists("last_run_state.json"):
        with open("last_run_state.json", "r") as f:
            state = json.load(f)
            return state.get("published_timestamp", 0), state.get("uuid", "")
    return 0, ""


def update_last_run_state(ts, uuid):
    with open("last_run_state.json", "w") as f:
        json.dump({"published_timestamp": ts, "uuid": uuid}, f)


def get_files_for_uuids(uuids):
    portal_url = "https://search.api.hubmapconsortium.org/v3/portal/search"
    response = requests.post(
        portal_url,
        json={"size": 10000, "query": {"ids": {"values": uuids}}, "_source": ["files"]},
        allow_redirects=False
    )
    if response.status_code == 303:
        response = requests.get(response.text.strip())
    response.raise_for_status()
    return {
        hit['_id']: [f['rel_path'] for f in hit['_source']['files']]
        for hit in response.json().get("hits", {}).get("hits", [])
    }

def get_content_length(url):
    try:
        response = requests.head(url, timeout=10)
        if response.status_code == 200:
            return int(response.headers.get("Content-Length", -1))
        else:
            logging.warning(f"HEAD request failed: {url}, status code: {response.status_code}")
    except Exception as e:
        logging.warning(f"Failed to get Content-Length for {url}: {e}")
    return -1

def is_valid_h5ad(path):
    try:
        ad.read_h5ad(path, backed='r')
        return True
    except Exception as e:
        logging.warning(f"Failed to open h5ad file {path}: {e}")
        return False

def retrieve_file(uuid, file_name="secondary_analysis.h5ad", outdir="data", max_retries=1):
    url = f"https://assets.hubmapconsortium.org/{uuid}/{file_name}"
    out_dir = os.path.join(outdir, uuid)
    os.makedirs(out_dir, exist_ok=True)
    file_path = os.path.join(out_dir, file_name)

    expected_size = get_content_length(url)

    if os.path.exists(file_path):
        actual_size = os.path.getsize(file_path)
        if expected_size > 0 and actual_size == expected_size and is_valid_h5ad(file_path):
            logging.info(f"[{uuid}] File exists and is valid, skipping download.")
            return file_path
        else:
            logging.warning(f"[{uuid}] Existing file invalid or incomplete, re-downloading.")
            os.remove(file_path)

    for attempt in range(max_retries + 1):
        try:
            resume = 0
            headers = {}
            if os.path.exists(file_path):
                resume = os.path.getsize(file_path)
                headers["Range"] = f"bytes={resume}-"

            with requests.get(url, headers=headers, stream=True, timeout=60) as r:
                r.raise_for_status()
                mode = 'ab' if resume > 0 else 'wb'
                with open(file_path, mode) as f:
                    for chunk in r.iter_content(chunk_size=1024 * 1024):
                        if chunk:
                            f.write(chunk)

            if expected_size > 0 and os.path.getsize(file_path) != expected_size:
                raise IOError(f"Incomplete download: {os.path.getsize(file_path)} vs {expected_size}")

            if not is_valid_h5ad(file_path):
                raise IOError("Downloaded file is corrupted or unreadable.")

            logging.info(f"[{uuid}] Downloaded and verified {file_path}")
            return file_path

        except Exception as e:
            logging.warning(f"[{uuid}] Attempt {attempt+1} failed: {e}")
            if os.path.exists(file_path):
                os.remove(file_path)
            time.sleep(3)

    raise RuntimeError(f"[{uuid}] All download attempts failed.")

def process_h5ad_to_index(file_path, uuid):
    if getattr(process_h5ad_to_index, "_seen", None) is None:
        process_h5ad_to_index._seen = set()

    if uuid in process_h5ad_to_index._seen:
        logging.info(f"Skipping already processed uuid: {uuid}")
        return
    process_h5ad_to_index._seen.add(uuid)

    global index_ct, index_dataset, first_dataset_ct, first_dataset_ds

    start = time.time()
    try:
        logging.info(f"[{uuid}] Reading {file_path}")
        log_memory_usage("START")
        adata = ad.read_h5ad(file_path)
        log_memory_usage("AFTER LOADING")
        logging.info(f"[{uuid}] Read .h5ad successful")
    except Exception as e:
        logging.error(f"[{uuid}] Failed to read .h5ad: {e}")
        raise
    if not (cell_type_label in adata.obs.columns and clid_label in adata.obs.columns and feature_name in adata.var.columns):
        raise ValueError(f"Missing required columns in {uuid}")

    adata.X = adata.layers.get("unspliced", adata.X)
    adata = adata[:, adata.var[feature_name].dropna().drop_duplicates().index]
    logging.info(f"{uuid} read + preprocess took {time.time() - start:.2f}s")

    for mode in ['cell_type', 'dataset']:
        start = time.time()
        idx = SCFind()
        label = cell_type_label if mode == 'cell_type' else 'dataset_id'
        adata.obs[label] = adata.obs.get(label, uuid)
        tissue = adata.uns.get('annotation_metadata', {}).get('azimuth_reference', {}).get('name', 'Unknown').capitalize() if mode == 'cell_type' else uuid

        idx.buildCellTypeIndex(
            adata=adata,
            tissue=tissue,
            dataset_id=uuid,
            feature_name=feature_name,
            cell_type_label=label,
            clid_label=clid_label,
            qb=2,
            if_expression=(mode == 'dataset'),
            raw_counts=True
        )
        log_memory_usage("AFTER INDEX BUILD")
        logging.info(f"{uuid} SCFind.buildCellTypeIndex ({mode}) took {time.time() - start:.2f}s")

        logging.info(f"Merging {mode} for {uuid} into index...")
        if mode == 'cell_type':
            if first_dataset_ct:
                index_ct = idx
                first_dataset_ct = False
            else:
                merge_start = time.time()
                index_ct.mergeDataset(idx)
                log_memory_usage("AFTER MERGE INDEX CT")
                logging.info(f"{uuid} merged into cell_type index in {time.time() - merge_start:.2f}s")
        else:
            if first_dataset_ds:
                index_dataset = idx
                first_dataset_ds = False
            else:
                merge_start = time.time()
                index_dataset.mergeDataset(idx)
                log_memory_usage("AFTER MERGE INDEX DS")
                logging.info(f"{uuid} merged into dataset index in {time.time() - merge_start:.2f}s")

    del adata, idx
    import gc
    gc.collect()
    log_memory_usage("AFTER CLEANUP")

    os.remove(file_path)
    dir_path = os.path.dirname(file_path)
    if not os.listdir(dir_path):
        os.rmdir(dir_path)
    logging.info(f"Processed and cleaned {uuid}")



def main():
    last_ts, last_uuid = get_last_run_state()
    all_results = []

    search_after = None
    while True:
        result = fetch_datasets_azimuth(last_ts, last_uuid, search_after)
        hits = result.get("hits", {}).get("hits", [])
        if not hits:
            break
        all_results.extend(hits)
        last_hit = hits[-1]
        search_after = last_hit['sort']
        last_ts = last_hit['_source']['published_timestamp']
        last_uuid = last_hit['_id']
    update_last_run_state(last_ts, last_uuid)

    sorted_hits = sorted(
        all_results,
        key=lambda h: (h['_source'].get("published_timestamp", 0), h['_id'])
    )

    tqdm.write(f"Total datasets to be processed: {len(sorted_hits)}")

    for hit in sorted_hits:
        ts = hit['_source'].get("published_timestamp", 0)
        tqdm.write(f"UUID: {hit['_id']}, published_timestamp: {ts}")

    uuids = [h['_id'] for h in sorted_hits]
    logging.info(f"Number of uuids: {len(uuids)}")
    if not uuids:
        logging.info("No new datasets found.")
        return

    uuid_to_files = get_files_for_uuids(uuids)
    any_updates = False
    processed_uuids = set()
    failed_uuids = []
    for uuid in tqdm(uuids, desc="Processing datasets", unit="dataset"):
        if uuid in processed_uuids:
            continue
        files = [f for f in uuid_to_files.get(uuid, []) if f.endswith("secondary_analysis.h5ad")]
        for file_name in files:
            try:
                path = retrieve_file(uuid, file_name)
                process_h5ad_to_index(path, uuid)
                processed_uuids.add(uuid)
                any_updates = True
            except Exception as e:
                logging.error(f"[{uuid}] Failed to process {file_name}: {e}")
                failed_uuids.append(uuid)
                continue
    if any_updates:
        index_ct.saveObject(index_new_path_celltype)
        index_dataset.saveObject(index_new_path_dataset)
        logging.info("Indexes saved locally.")
        log_memory_usage("AFTER SAVE OBJECT LOCALLY")
    

        upload_index_to_s3_with_tags(index_new_path_celltype, BUCKET_NAME, f"{S3_PREFIX}/cell_type/index.bin",
                                    tags={"version": timestamp, "type": "cell_type"})
        upload_index_to_s3_with_tags(index_new_path_dataset, BUCKET_NAME, f"{S3_PREFIX}/dataset/index.bin",
                                    tags={"version": timestamp, "type": "dataset"})
        save_index_metadata_json(ct_version_id, ds_version_id, BUCKET_NAME, S3_PREFIX)

        save_all_index_versions_metadata_to_s3(
            BUCKET_NAME,
            keys=[f"{S3_PREFIX}/cell_type/index.bin", f"{S3_PREFIX}/dataset/index.bin"],
            prefix=S3_PREFIX
        )

        newest_ts = max(hit['_source'].get("published_timestamp", 0) for hit in all_results)
    else:
        logging.info("No new datasets. Skipped saving or uploading updated index.")


if __name__ == "__main__":
    main()
