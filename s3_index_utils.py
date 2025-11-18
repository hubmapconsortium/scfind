
import tempfile
import os
import boto3
import logging
import sys
import json
from scfind import SCFind
import botocore.exceptions

def get_latest_s3_object(bucket, key):
    s3 = boto3.client('s3')
    try:
        versions = s3.list_object_versions(Bucket=bucket, Prefix=key)
    except botocore.exceptions.ClientError as e:
        if e.response['Error']['Code'] == 'AllAccessDisabled':
            logging.warning("Versioning access disabled or object doesn't exist.")
            return None, None
        raise

    version_list = versions.get('Versions', [])
    if not version_list:
        logging.info(f"No existing versions found for {key} in S3.")
        return None, None

    latest = sorted(version_list, key=lambda x: x['LastModified'], reverse=True)[0]
    version_id = latest['VersionId']
    logging.info(f"Latest S3 version for {key}: {version_id}")
    return version_id, latest


def download_s3_index(bucket, key, local_path):
    version_id, _ = get_latest_s3_object(bucket, key)
    if version_id is None:
        return None

    s3 = boto3.client('s3')
    s3.download_file(Bucket=bucket, Key=key, Filename=local_path, ExtraArgs={'VersionId': version_id})
    logging.info(f"Downloaded latest index from S3: {key} → {local_path}")
    return version_id

def load_s3_index_to_scfind(bucket, key):
    s3 = boto3.client('s3')

    try:
        versions = s3.list_object_versions(Bucket=bucket, Prefix=key)
    except botocore.exceptions.ClientError as e:
        if e.response['Error']['Code'] == 'AllAccessDisabled':
            logging.warning("Versioning access disabled or object doesn't exist.")
            return SCFind(), None
        raise

    version_list = [v for v in versions.get('Versions', []) if not v.get('IsDeleteMarker')]
    if not version_list:
        logging.info(f"No index found at s3://{bucket}/{key}")
        return SCFind(), None

    latest = sorted(version_list, key=lambda x: x['LastModified'], reverse=True)[0]
    version_id = latest['VersionId']
    logging.info(f"Loading S3 object: {key} (version: {version_id})")
    with tempfile.NamedTemporaryFile(delete=False) as tmp:
        temp_path = tmp.name

    try:
        s3.download_file(Bucket=bucket, Key=key, Filename=temp_path, ExtraArgs={'VersionId': version_id})
        sc = SCFind()
        sc.loadObject(temp_path)
        logging.info(f"Loaded index from temporary file {temp_path}")
        return sc, version_id
    finally:
        os.remove(temp_path)
        logging.info(f"Temporary file deleted: {temp_path}")

def upload_index_to_s3_with_tags(local_path, bucket, key, tags: dict):
    s3 = boto3.client('s3')
    tag_str = '&'.join(f"{k}={v}" for k, v in tags.items())
    s3.upload_file(
        Filename=local_path,
        Bucket=bucket,
        Key=key,
        ExtraArgs={'Tagging': tag_str}
    )
    logging.info(f"Uploaded to s3://{bucket}/{key} with tags: {tags}")

def save_index_metadata_json(cell_type_id, dataset_id, bucket, prefix, out_path="index_metadata.json"):
    metadata = {
        "cell_type": {
            "latest_version": cell_type_id,
            "s3_key": f"{prefix}/cell_type/index.bin"
        },
        "dataset": {
            "latest_version": dataset_id,
            "s3_key": f"{prefix}/dataset/index.bin"
        }
    }
    with open(out_path, "w") as f:
        json.dump(metadata, f, indent=2)
    logging.info(f"Saved index metadata to {out_path}")

    s3 = boto3.client('s3')
    key = f"{prefix}/index_metadata.json" if prefix else "index_metadata.json"
    s3.upload_file(Filename=out_path, Bucket=bucket, Key=key)
    logging.info(f"Uploaded metadata to s3://{bucket}/{key}")


def list_all_index_versions(bucket, key):
    s3 = boto3.client('s3')
    versions = s3.list_object_versions(Bucket=bucket, Prefix=key).get('Versions', [])
    result = []
    for v in versions:
        if not v.get('IsDeleteMarker'):
            tag_resp = s3.get_object_tagging(Bucket=bucket, Key=key, VersionId=v['VersionId'])
            result.append({
                "version_id": v['VersionId'],
                "last_modified": v['LastModified'].isoformat(),
                "tags": {t['Key']: t['Value'] for t in tag_resp['TagSet']}
            })
    return result

def save_all_index_versions_metadata_to_s3(bucket, keys: list[str], prefix="", out_path="all_index_versions.json"):
    import boto3, json, logging
    s3 = boto3.client('s3')
    all_versions = []

    for key in keys:
        versions = list_all_index_versions(bucket, key)
        for v in versions:
            v["source_key"] = key
        all_versions.extend(versions)

    version_map = {}
    for v in all_versions:
        version_tag = v["tags"].get("version")
        index_type = v["tags"].get("type") 
        if not version_tag or not index_type:
            continue

        if version_tag not in version_map:
            version_map[version_tag] = {"tags": {"version": version_tag}}

        entry = {
            "key": v["source_key"],
            "version_id": v["version_id"]
        }
        if index_type == "cell_type":
            version_map[version_tag]["index_with_datasets.bin"] = entry
        elif index_type == "dataset":
            version_map[version_tag]["index_datasets_no_celltype.bin"] = entry

    sorted_versions = sorted(version_map.keys())
    for i, vtag in enumerate(sorted_versions):
        vdata = version_map[vtag]

        if "index_with_datasets.bin" not in vdata and i > 0:
            prev = version_map[sorted_versions[i - 1]]
            if "index_with_datasets.bin" in prev:
                vdata["index_with_datasets.bin"] = prev["index_with_datasets.bin"]

        if "index_datasets_no_celltype.bin" not in vdata and i > 0:
            prev = version_map[sorted_versions[i - 1]]
            if "index_datasets_no_celltype.bin" in prev:
                vdata["index_datasets_no_celltype.bin"] = prev["index_datasets_no_celltype.bin"]

    metadata = {
        "latest": sorted_versions[-1] if sorted_versions else None,
        "versions": version_map
    }

    with open(out_path, "w") as f:
        json.dump(metadata, f, indent=2)
    logging.info(f"Saved all version metadata to {out_path}")

    s3_key = f"{prefix}/all_index_versions.json" if prefix else "all_index_versions.json"
    s3.upload_file(Filename=out_path, Bucket=bucket, Key=s3_key)
    logging.info(f"Uploaded version metadata to s3://{bucket}/{s3_key}")
