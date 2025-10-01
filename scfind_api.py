
from __future__ import annotations

import os
import json
import time
import uuid
import gc
import ctypes
import logging
import traceback
from typing import Any, Dict, Optional, Tuple
from collections import defaultdict
from contextlib import contextmanager
from datetime import datetime, timezone
from threading import Lock

import boto3
import psutil
from botocore.exceptions import ClientError
from cachetools import TTLCache
from flask import Flask, jsonify, request, g
from flask_cors import CORS
from filelock import FileLock
from logging.handlers import RotatingFileHandler

import scfind

from logging import Formatter, StreamHandler, getLogger
try:
    # Safe across gunicorn workers (processes/threads)
    from concurrent_log_handler import ConcurrentRotatingFileHandler as SafeRotating
except Exception:
    # Fallback if the dependency isn't available (not process-safe)
    from logging.handlers import RotatingFileHandler as SafeRotating

app = Flask(__name__)

log_formatter = Formatter('%(asctime)s - %(levelname)s - %(message)s')
access_line_formatter = Formatter('%(message)s')

# File handlers (process-safe rotation)
mem_handler = SafeRotating("memory.log", maxBytes=2 * 1024 * 1024, backupCount=5, delay=True)
mem_handler.setFormatter(log_formatter)
mem_handler.setLevel(logging.INFO)

access_handler = SafeRotating("access.log", maxBytes=10 * 1024 * 1024, backupCount=10, delay=True)
access_handler.setFormatter(access_line_formatter)
access_handler.setLevel(logging.INFO)

# Also mirror to stdout so `docker logs -f` shows everything
stream_handler = StreamHandler()
stream_handler.setFormatter(log_formatter)
stream_handler.setLevel(logging.INFO)

# app logger (memory + diagnostics)
app.logger.handlers[:] = [mem_handler, stream_handler]
app.logger.setLevel(logging.INFO)
app.logger.propagate = False

# access logger (JSON lines you write in before/after/teardown)
access_logger = getLogger("access")
access_logger.handlers[:] = [access_handler, stream_handler]
access_logger.setLevel(logging.INFO)
access_logger.propagate = False

CORS(app, resources={r"/api/*": {"origins": [
    "http://localhost:5001",
    "http://localhost:6006",
    "https://scfind.dev.hubmapconsortium.org",
    "https://scfind.hubmapconsortium.org",
    "https://portal.dev.hubmapconsortium.org",
    "https://portal.test.hubmapconsortium.org",
    "https://portal-prod.test.hubmapconsortium.org",
    "https://portal.hubmapconsortium.org",
]}}, supports_credentials=True)

S3_BUCKET_NAME = os.environ.get('SCFIND_S3_BUCKET', 'scfind-dataset')
DATA_ROOT = os.environ.get('SCFIND_DATA_ROOT', '/data/scfind') 
CACHE_MAXSIZE = int(os.environ.get('SCFIND_CACHE_MAXSIZE', '2'))
CACHE_TTL = int(os.environ.get('SCFIND_CACHE_TTL_SECONDS', '36000'))  # 0 => no TTL

os.makedirs(DATA_ROOT, exist_ok=True)

# cache: index_version -> IndexHolder
if CACHE_TTL > 0:
    scfind_cache: TTLCache[str, "IndexHolder"] = TTLCache(maxsize=CACHE_MAXSIZE, ttl=CACHE_TTL)
else:
    scfind_cache: Dict[str, "IndexHolder"] = {}

scfind_cache_lock = Lock()               # protects scfind_cache / cache_meta
loading_locks: Dict[str, Lock] = defaultdict(Lock)  # per canonical_key
active_uses: Dict[str, int] = defaultdict(int)

# metadata keyed by friendly index_version
cache_meta: Dict[str, Dict[str, Any]] = {}

_SENSITIVE_KEYS = {"authorization", "api_key", "x-api-key", "password", "secret", "token"}


def now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def rss_mb() -> float:
    return psutil.Process(os.getpid()).memory_info().rss / 1e6


def malloc_trim() -> None:
    try:
        ctypes.CDLL("libc.so.6").malloc_trim(0)
    except Exception:
        pass

def _clip_str(s: str, limit=2000):
    return s if len(s) <= limit else s[:limit] + "...<clipped>"

def safe_json(val, limit=2000):
    try:
        if isinstance(val, (str, int, float, bool)) or val is None:
            return _clip_str(str(val), limit) if isinstance(val, str) else val
        if isinstance(val, (list, tuple)):
            return [safe_json(v, limit) for v in val]
        if isinstance(val, dict):
            return {str(k): safe_json(v, limit) for k, v in val.items()}
        return _clip_str(str(val), limit)
    except Exception:
        return "<unserializable>"


def _mask_sensitive(d: dict):
    if not isinstance(d, dict):
        return d
    return {k: ("***" if str(k).lower() in _SENSITIVE_KEYS else v) for k, v in d.items()}

@app.before_request
def _access_log_request():
    g._start_ts = time.time()
    g.request_id = request.headers.get("X-Request-ID") or str(uuid.uuid4())
    g.request_timestamp = now_iso()
    g.endpoint_name = request.endpoint

    # merged params
    args = request.args.to_dict(flat=False)
    args = {k: (v[0] if isinstance(v, list) and len(v) == 1 else v) for k, v in args.items()}
    body = request.get_json(silent=True)
    if isinstance(body, dict):
        args.update(body)
    params = _mask_sensitive(safe_json(args))

    # summarize a few
    watch_keys = {"index_version", "gene_list", "cell_types", "datasets", "dataset", "cell_type"}
    extras = {k: params.get(k) for k in watch_keys if isinstance(params, dict) and k in params}

    access_logger.info(json.dumps({
        "event": "request",
        "request_id": g.request_id,
        "request_timestamp": g.request_timestamp,
        "method": request.method,
        "path": request.path,
        "endpoint": g.endpoint_name,
        "remote_addr": request.headers.get("X-Forwarded-For", request.remote_addr),
        "content_length": request.content_length,
        "user_agent": request.headers.get("User-Agent"),
        "params": params,
        "extras": extras,
    }, ensure_ascii=False))

@app.after_request
def _access_log_response(resp):
    end_ts = time.time()
    latency_ms = round((end_ts - getattr(g, "_start_ts", end_ts)) * 1000.0, 1)
    resp.headers["X-Request-ID"] = getattr(g, "request_id", "-")

    access_logger.info(json.dumps({
        "event": "response",
        "request_id": getattr(g, "request_id", "-"),
        "request_timestamp": getattr(g, "request_timestamp", None),
        "response_timestamp": now_iso(),
        "method": request.method,
        "path": request.path,
        "endpoint": getattr(g, "endpoint_name", None),
        "status_code": resp.status_code,
        "latency_ms": latency_ms,
        "resolved_index_version": getattr(g, "resolved_index_version", None),
    }, ensure_ascii=False))
    return resp

@app.teardown_request
def _access_log_teardown(exc):
    if exc is not None:
        end_ts = time.time()
        latency_ms = round((end_ts - getattr(g, "_start_ts", end_ts)) * 1000.0, 1)
        access_logger.error(json.dumps({
            "event": "exception",
            "request_id": getattr(g, "request_id", "-"),
            "request_timestamp": getattr(g, "request_timestamp", None),
            "exception_type": type(exc).__name__,
            "exception_msg": str(exc),
            "traceback": "".join(traceback.format_exception(type(exc), exc, exc.__traceback__)),
            "method": request.method,
            "path": request.path,
            "endpoint": getattr(g, "endpoint_name", None),
            "latency_ms": latency_ms,
        }, ensure_ascii=False))

def _read_versions_manifest() -> Dict[str, Any]:
    s3 = boto3.client('s3')
    obj = s3.get_object(Bucket=S3_BUCKET_NAME, Key='prod_index/all_index_versions.json')
    content = obj['Body'].read().decode('utf-8')
    return json.loads(content)

def s3_version_info(index_version: Optional[str]) -> Tuple[str, str, Tuple[str, str], Tuple[str, str]]:
    meta = _read_versions_manifest()
    if index_version is None:
        index_version = meta.get("latest")
    if not index_version or index_version not in meta.get("versions", {}):
        raise ValueError(f"Invalid or missing index_version: {index_version}")
    v = meta["versions"][index_version]
    f1, f2 = v["index_with_datasets.bin"], v["index_datasets_no_celltype.bin"]
    canonical_key = f"{index_version}:{f1['version_id']}:{f2['version_id']}"
    return index_version, canonical_key, (f1["key"], f1["version_id"]), (f2["key"], f2["version_id"])

def ensure_local_files(canonical_key: str, f1: Tuple[str, str], f2: Tuple[str, str]) -> Tuple[str, str]:
    d = os.path.join(DATA_ROOT, canonical_key)
    os.makedirs(d, exist_ok=True)
    p1 = os.path.join(d, "index_with_datasets.bin")
    p2 = os.path.join(d, "index_datasets_no_celltype.bin")

    for (key, vid), dst in ((f1, p1), (f2, p2)):
        lock_path = dst + ".lock"
        with FileLock(lock_path):
            if not os.path.exists(dst):
                tmp = dst + ".part"
                s3 = boto3.client('s3')
                s3.download_file(S3_BUCKET_NAME, key, tmp, ExtraArgs={'VersionId': vid})
                os.replace(tmp, dst)  # atomic
    return p1, p2

class IndexHolder:
    def __init__(self, a: scfind.SCFind, b: scfind.SCFind, canonical_key: str, index_version: str, load_uid: str):
        self.a = a
        self.b = b
        self.canonical_key = canonical_key
        self.index_version = index_version
        self.load_uid = load_uid

    def close(self):
        for obj in (self.a, self.b):
            if hasattr(obj, "close"):
                try:
                    obj.close()
                except Exception:
                    pass

def _dispose(holder: Optional[IndexHolder]) -> None:
    if not holder:
        return
    try:
        holder.close()
    except Exception:
        pass
    # Drop strong refs
    del holder
    gc.collect()
    malloc_trim()

def resolve_index_version() -> str:
    iv = request.args.get('index_version')
    if not iv:
        body = request.get_json(silent=True) or {}
        iv = body.get('index_version')
    if not iv:
        # fallback to latest
        iv = s3_version_info(None)[0]
    g.resolved_index_version = iv
    return iv

@contextmanager
def use_dataset(index_version: str):
    active_uses[index_version] += 1
    try:
        yield
    finally:
        active_uses[index_version] -= 1

def get_scfind(index_version: Optional[str] = None) -> Tuple[scfind.SCFind, scfind.SCFind]:
    index_version, canonical_key, f1, f2 = s3_version_info(index_version)

    # fast path
    with scfind_cache_lock:
        holder = scfind_cache.get(index_version)
        if holder:
            return holder.a, holder.b

    # serialize loads per canonical resource
    with loading_locks[canonical_key]:
        with scfind_cache_lock:
            holder = scfind_cache.get(index_version)
            if holder:
                return holder.a, holder.b

        # ensure files exist on disk
        p1, p2 = ensure_local_files(canonical_key, f1, f2)

        # load safely (so partial failures don't leak)
        load_uid = uuid.uuid4().hex
        rss_before = rss_mb()
        a = b = None
        try:
            a = scfind.SCFind(); a.loadObject(p1)
            b = scfind.SCFind(); b.loadObject(p2)
        except Exception:
            # best-effort cleanup of partially constructed objects
            try:
                if a and hasattr(a, "close"): a.close()
            except Exception:
                pass
            try:
                if b and hasattr(b, "close"): b.close()
            except Exception:
                pass
            raise

        rss_after = rss_mb()
        holder = IndexHolder(a, b, canonical_key, index_version, load_uid)
        app.logger.info(
            f"[Cache] Loaded {index_version} (ckey={canonical_key}) "
            f"rss_before={rss_before:.2f}MB rss_after={rss_after:.2f}MB "
            f"delta={rss_after - rss_before:.2f}MB"
        )

        # insert with eviction if needed (dispose the *victim*, not the new holder)
        with scfind_cache_lock:
            def evict_one():
                # pick any not-in-use entry to evict
                for victim_k in list(scfind_cache.keys()):
                    if active_uses.get(victim_k, 0) == 0:
                        victim = scfind_cache.pop(victim_k, None)
                        cache_meta.pop(victim_k, None)
                        if victim:
                            before = rss_mb()
                            _dispose(victim)
                            after = rss_mb()
                            app.logger.info(
                                f"[Cache] Evicted {victim_k} to make room (not in use)"
                            )
                            app.logger.info(
                                f"[UnloadDelta] iv={victim_k} "
                                f"before={before:.2f}MB after={after:.2f}MB "
                                f"delta={after - before:.2f}MB"
                            )
                        return True
                return False

            # respect capacity for both TTLCache and dict
            maxsize = scfind_cache.maxsize if isinstance(scfind_cache, TTLCache) else CACHE_MAXSIZE
            while len(scfind_cache) >= maxsize:
                if not evict_one():
                    # nothing eligible to evict; proceed (rare, but avoid deadlock)
                    break

            scfind_cache[index_version] = holder
            cache_meta[index_version] = {
                "canonical_key": canonical_key,
                "loaded_at": now_iso(),
                "aux": {"paths": [p1, p2]},
            }

        return holder.a, holder.b

@app.errorhandler(MemoryError)
def handle_memory_error(e):
    return jsonify({"error": str(e)}), 503

@app.route('/api/cacheStatus', methods=['GET'])
def cache_status():
    with scfind_cache_lock:
        return jsonify({
            "cached_versions": [str(k) for k in scfind_cache.keys()],
            "cache_size": len(scfind_cache),
            "cache_maxsize": CACHE_MAXSIZE,
            "active_uses": {str(k): v for k, v in active_uses.items()},
            "memory_mb": rss_mb(),
        })

@app.route('/api/diag/mem', methods=['GET'])
def diag_mem():
    before = rss_mb()
    gc.collect(); malloc_trim()
    after = rss_mb()
    return jsonify({"ok": True, "rss_mb_before": round(before, 2), "rss_mb_after": round(after, 2)})

@app.route('/api/memoryDetail', methods=['GET'])
def memory_detail():
    try:
        proc = psutil.Process(os.getpid())
        maps = []
        try:
            for m in proc.memory_maps():
                maps.append({"path": m.path, "rss_mb": round(getattr(m, 'rss', 0) / 1e6, 2)})
            maps.sort(key=lambda x: x["rss_mb"], reverse=True)
            maps = maps[:20]
        except Exception:
            maps = []
        with scfind_cache_lock:
            per_index = []
            for k in scfind_cache.keys():
                meta = cache_meta.get(k, {})
                per_index.append({
                    "index_version": str(k),
                    "active_uses": active_uses.get(k, 0),
                    "loaded_at": meta.get("loaded_at"),
                    "paths": meta.get("aux", {}).get("paths"),
                    "canonical_key": meta.get("canonical_key"),
                })
        return jsonify({
            "process_rss_mb": round(proc.memory_info().rss/1e6, 2),
            "cache_size": len(scfind_cache),
            "cache_maxsize": CACHE_MAXSIZE,
            "active_uses": {str(k): v for k, v in active_uses.items()},
            "loaded_indexes": per_index,
            "top_mmaps": maps,
        })
    except Exception as e:
        return jsonify({"error": str(e)}), 500

@app.route('/api/unloadIndex', methods=['POST'])
def unload_index():
    data = request.get_json(silent=True) or {}
    index_version = data.get('index_version') or request.args.get('index_version')
    delete_files = str(data.get('delete_files') or request.args.get('delete_files') or "false").lower() in ("1", "true", "yes")

    if not index_version:
        return jsonify({"error": "missing index_version"}), 400

    with scfind_cache_lock:
        if active_uses.get(index_version, 0) > 0:
            return jsonify({"error": f"Index {index_version} is in use ({active_uses.get(index_version)})."}), 409
        holder = scfind_cache.pop(index_version, None)
        meta = cache_meta.pop(index_version, None)

    before = rss_mb()
    _dispose(holder)
    after = rss_mb()

    deleted = []
    if delete_files and meta:
        for p in meta.get("aux", {}).get("paths", []) or []:
            try:
                if os.path.exists(p):
                    os.remove(p)
                    deleted.append(p)
            except Exception:
                app.logger.warning(f"[Unload] Could not delete {p}")

    return jsonify({
        "unloaded": bool(holder),
        "index_version": index_version,
        "canonical_key": (meta or {}).get("canonical_key"),
        "rss_mb_before": round(before, 2),
        "rss_mb_after": round(after, 2),
        "deleted_files": deleted,
    })

def wrap_with_use_dataset(route_func):
    def wrapper(*args, **kwargs):
        iv = resolve_index_version()
        with use_dataset(iv):
            return route_func(*args, **kwargs)
    wrapper.__name__ = route_func.__name__
    return wrapper

@app.route('/api/findDatasetForCellType', methods=['GET', 'POST'])
@wrap_with_use_dataset
def find_dataset_for_cell_type_api():
    index_version = g.resolved_index_version
    a, _ = get_scfind(index_version)

    cell_type = request.get_json().get('cell_type') if request.method == 'POST' else request.args.get('cell_type')
    if not cell_type:
        return jsonify({"error": "Missing 'cell_type'"}), 400

    datasets, cell_counts = a.find_dataset_for_cell_type(cell_type)
    cell_counts = [int(x) for x in cell_counts]
    return jsonify({"datasets": datasets, "counts": cell_counts})

@app.route('/api/cellTypeNames', methods=['GET', 'POST'])
@wrap_with_use_dataset
def get_cellTypeNames():
    index_version = g.resolved_index_version
    a, _ = get_scfind(index_version)
    return jsonify({"cellTypeNames": a.cellTypeNames()})

@app.route('/api/marker_genes', methods=['GET', 'POST'])
@wrap_with_use_dataset
def get_marker_genes():
    index_version = g.resolved_index_version
    a, _ = get_scfind(index_version)

    marker_genes = request.args.get('marker_genes').split(',') if request.method == 'GET' else request.get_json().get('marker_genes')
    dataset_name = request.args.get('dataset_name') if request.method == 'GET' else (request.get_json().get('dataset_name'))
    if isinstance(dataset_name, str):
        dataset_name = dataset_name.split(',')

    res = a.markerGenes(marker_genes, dataset_name)
    if isinstance(res, dict):
        return jsonify({"findGeneSignatures": res})
    if hasattr(res, "to_dict"):
        return jsonify({"findGeneSignatures": res.to_dict(orient='records')})
    return jsonify({"error": "Unexpected data type"}), 400

@app.route('/api/cellTypeMarkers', methods=['GET', 'POST'])
@wrap_with_use_dataset
def get_cellTypeMarkers():
    index_version = g.resolved_index_version
    a, _ = get_scfind(index_version)

    if request.method == 'POST':
        data = request.get_json()
        cell_types = data.get('cell_types')
        background_cell_types = data.get('background_cell_types')
        top_k = data.get('top_k', 5)
        sort_field = data.get('sort_field', 'f1')
        include_prefix = data.get('include_prefix', True)
    else:
        cell_types = request.args.get('cell_types')
        if cell_types:
            cell_types = cell_types.split(',')
        background_cell_types = request.args.get('background_cell_types')
        if background_cell_types:
            background_cell_types = background_cell_types.split(',')
        top_k = request.args.get('top_k', default=5, type=int)
        sort_field = request.args.get('sort_field', default='f1', type=str)
        include_prefix = request.args.get('include_prefix', default=True, type=bool)

    if not cell_types:
        return jsonify({"error": "Missing 'cell_types'"}), 400

    res = a.cellTypeMarkers(cell_types=cell_types, background_cell_types=background_cell_types,
                            top_k=top_k, sort_field=sort_field, include_prefix=include_prefix)
    if isinstance(res, dict):
        return jsonify({"findGeneSignatures": res})
    if hasattr(res, "to_dict"):
        return jsonify({"findGeneSignatures": res.to_dict(orient='records')})
    return jsonify({"error": "Unexpected data type"}), 400

@app.route('/api/evaluateMarkers', methods=['GET', 'POST'])
@wrap_with_use_dataset
def get_evaluateMarkers():
    index_version = g.resolved_index_version
    a, _ = get_scfind(index_version)

    if request.method == 'GET':
        gene_list = request.args.get('gene_list').split(',')
        cell_types = request.args.get('cell_types').split(',')
        background_cell_types = request.args.get('background_cell_types')
        background_cell_types = background_cell_types.split(',') if background_cell_types else None
        sort_field = request.args.get('sort_field', default='f1', type=str)
        include_prefix = request.args.get('include_prefix', default=True, type=bool)
    else:
        data = request.get_json()
        gene_list = data.get('gene_list')
        cell_types = data.get('cell_types')
        background_cell_types = data.get('background_cell_types')
        sort_field = data.get('sort_field', 'f1')
        include_prefix = data.get('include_prefix', True)

    res = a.evaluateMarkers(gene_list, cell_types, background_cell_types, sort_field, include_prefix)
    if isinstance(res, dict):
        return jsonify({"findGeneSignatures": res})
    if hasattr(res, "to_dict"):
        return jsonify({"findGeneSignatures": res.to_dict(orient='records')})
    return jsonify({"error": "Unexpected data type"}), 400

@app.route('/api/hyperQueryCellTypes', methods=['GET', 'POST'])
@wrap_with_use_dataset
def get_hyperQueryCellTypes():
    index_version = g.resolved_index_version
    a, _ = get_scfind(index_version)

    if request.method == 'GET':
        gene_list = request.args.get('gene_list').split(',')
        dataset_name = request.args.get('dataset_name')
        dataset_name = dataset_name.split(',') if dataset_name else None
        include_prefix = request.args.get('include_prefix', default=True, type=bool)
    else:
        data = request.get_json()
        gene_list = data.get('gene_list')
        dataset_name = data.get('dataset_name')
        include_prefix = data.get('include_prefix', True)

    res = a.hyperQueryCellTypes(gene_list, dataset_name, include_prefix)
    if isinstance(res, dict):
        return jsonify({"findGeneSignatures": res})
    if hasattr(res, "to_dict"):
        return jsonify({"findGeneSignatures": res.to_dict(orient='records')})
    return jsonify({"error": "Unexpected data type"}), 400

@app.route('/api/findCellTypeSpecificities', methods=['GET', 'POST'])
@wrap_with_use_dataset
def findCellTypeSpecificities():
    index_version = g.resolved_index_version
    a, _ = get_scfind(index_version)

    if request.method == 'GET':
        gene_list = request.args.get('gene_list')
        gene_list = gene_list.split(',') if gene_list else None
        datasets = request.args.get('datasets')
        datasets = datasets.split(',') if datasets else None
        min_cells = request.args.get('min_cells', default=10, type=int)
        min_fraction = request.args.get('min_fraction', default=0.25, type=float)
    else:
        data = request.get_json()
        gene_list = data.get('gene_list')
        datasets = data.get('datasets')
        min_cells = data.get('min_cells', 10)
        min_fraction = data.get('min_fraction', 0.25)

    res = a.findCellTypeSpecificities(gene_list=gene_list, datasets=datasets,
                                      min_cells=min_cells, min_fraction=min_fraction)
    if isinstance(res, dict):
        return jsonify({"findGeneSignatures": res})
    if hasattr(res, "to_dict"):
        return jsonify({"findGeneSignatures": res.to_dict(orient='records')})
    return jsonify({"error": "Unexpected data type"}), 400

@app.route('/api/findTissueSpecificities', methods=['GET', 'POST'])
@wrap_with_use_dataset
def findTissueSpecificities():
    index_version = g.resolved_index_version
    a, _ = get_scfind(index_version)

    if request.method == 'GET':
        gene_list = request.args.get('gene_list')
        gene_list = gene_list.split(',') if gene_list else None
        min_cells = request.args.get('min_cells', default=10, type=int)
    else:
        data = request.get_json()
        gene_list = data.get('gene_list')
        min_cells = data.get('min_cells', 10)

    res = a.findCellTypeSpecificities(gene_list=gene_list, min_cells=min_cells)
    if isinstance(res, dict):
        return jsonify({"findGeneSignatures": res})
    if hasattr(res, "to_dict"):
        return jsonify({"findGeneSignatures": res.to_dict(orient='records')})
    return jsonify({"error": "Unexpected data type"}), 400

@app.route('/api/findHouseKeepingGenes', methods=['GET', 'POST'])
@wrap_with_use_dataset
def findHouseKeepingGenes():
    index_version = g.resolved_index_version
    a, _ = get_scfind(index_version)

    if request.method == 'GET':
        cell_types = request.args.get('cell_types').split(',')
        min_recall = request.args.get('min_recall', default=0.5, type=float)
        max_genes = request.args.get('max_genes', default=1000, type=int)
    else:
        data = request.get_json()
        cell_types = data.get('cell_types')
        min_recall = data.get('min_recall', 0.5)
        max_genes = data.get('max_genes', 1000)

    res = a.findHouseKeepingGenes(cell_types, min_recall, max_genes)
    if isinstance(res, dict):
        return jsonify({"findGeneSignatures": res})
    if hasattr(res, "to_dict"):
        return jsonify({"findGeneSignatures": res.to_dict(orient='records')})
    if isinstance(res, list):
        return jsonify({"findGeneSignatures": res})
    if isinstance(res, str):
        return jsonify({"findGeneSignatures": {"message": res}})
    return jsonify({"error": f"Unexpected data type: {type(res)}"}), 400

@app.route('/api/findGeneSignatures', methods=['GET', 'POST'])
@wrap_with_use_dataset
def get_findGeneSignatures():
    index_version = g.resolved_index_version
    a, _ = get_scfind(index_version)

    if request.method == 'GET':
        cell_types = request.args.get('cell_types')
        cell_types = cell_types.split(',') if cell_types else None
        min_cells = request.args.get('min_cells', default=10, type=int)
        max_genes = request.args.get('max_genes', default=1000, type=int)
        max_pval = request.args.get('max_pval', default=0, type=float)
    else:
        data = request.get_json()
        cell_types = data.get('cell_types')
        min_cells = data.get('min_cells', 10)
        max_genes = data.get('max_genes', 1000)
        max_pval = data.get('max_pval', 0)

    res = a.findGeneSignatures(cell_types, min_cells, max_genes, min_cells, max_pval)
    if isinstance(res, dict):
        return jsonify({"findGeneSignatures": res})
    if hasattr(res, "to_dict"):
        return jsonify({"findGeneSignatures": res.to_dict(orient='records')})
    return jsonify({"error": "Unexpected data type"}), 400

@app.route('/api/cellTypeCountForTissue', methods=['GET', 'POST'])
@wrap_with_use_dataset
def get_cellTypeCountForTissue():
    index_version = g.resolved_index_version
    a, _ = get_scfind(index_version)

    tissue = (request.get_json().get('tissue') if request.method == 'POST' else request.args.get('tissue'))
    if not tissue or not isinstance(tissue, str):
        return jsonify({"error": "Missing or invalid 'tissue'"}), 400

    result_df = a.cellTypeCountForTissue(tissue).reset_index()
    return jsonify({"cellTypeCounts": result_df.to_dict(orient='records')})

@app.route('/api/CLID2CellType', methods=['GET', 'POST'])
@wrap_with_use_dataset
def get_CLID2CellType():
    index_version = g.resolved_index_version
    a, _ = get_scfind(index_version)

    clid_label = (request.get_json().get('CLID_label') if request.method == 'POST' else request.args.get('CLID_label'))
    if not clid_label:
        return jsonify({"error": "Missing 'CLID_label'"}), 400
    result = a.CLID2CellType(clid_label)
    return jsonify({"cell_types": list(result)})

@app.route('/api/CellType2CLID', methods=['GET', 'POST'])
@wrap_with_use_dataset
def get_CellType2CLID():
    index_version = g.resolved_index_version
    a, _ = get_scfind(index_version)

    cell_type = (request.get_json().get('cell_type') if request.method == 'POST' else request.args.get('cell_type'))
    if not cell_type:
        return jsonify({"error": "Missing 'cell_type'"}), 400
    result = a.CellType2CLID(cell_type)
    return jsonify({"CLIDs": list(result)})

@app.route('/api/getDatasets', methods=['GET', 'POST'])
@wrap_with_use_dataset
def get_datasets():
    index_version = g.resolved_index_version
    a, _ = get_scfind(index_version)

    datasets, counts = b.getDatasets()

    # Ensure JSON-serializable ints
    counts = [int(c) for c in counts]

    return jsonify({
        "datasets": datasets,
        "counts": counts
    })

@app.route('/api/scfindGenes', methods=['GET', 'POST'])
@wrap_with_use_dataset
def get_scfind_genes():
    index_version = g.resolved_index_version
    a, _ = get_scfind(index_version)
    return jsonify({"genes": a.scfindGenes})

@app.route('/api/clidMappingAll', methods=['GET', 'POST'])
@wrap_with_use_dataset
def get_clid_mapping_all():
    index_version = g.resolved_index_version
    a, _ = get_scfind(index_version)
    clid_mapping_raw = a.getCLIDmappingAll()
    clid_mapping = {k: list(v) for k, v in clid_mapping_raw.items()}
    return jsonify({"clidMappingAll": clid_mapping})

@app.route('/api/findDatasets', methods=['GET', 'POST'])
@wrap_with_use_dataset
def get_findDatasets():
    index_version = g.resolved_index_version
    _, b = get_scfind(index_version)

    if request.method == 'POST':
        data = request.get_json()
        gene_list = data.get('gene_list')
        datasets = data.get('datasets')
        min_cells = data.get('min_cells', 10)
        if not gene_list:
            return jsonify({"error": "Missing gene_list"}), 400
    else:
        gene_list = request.args.get('gene_list')
        datasets = request.args.get('datasets')
        min_cells = request.args.get('min_cells', default=10, type=int)
        if not gene_list:
            return jsonify({"error": "Missing gene_list"}), 400

    if isinstance(gene_list, str):
        gene_list = [g.strip() for g in gene_list.split(',')]
    if isinstance(datasets, str):
        datasets = [d.strip() for d in datasets.split(',')]

    datasets_by_gene, counts_by_gene = b.findDatasets(
        gene_list=gene_list, min_cells=min_cells, datasets=datasets
    )
    return jsonify({
        "findDatasets": datasets_by_gene,  
        "counts": counts_by_gene          
    })

@app.route('/api/getCellTypeExpression', methods=['GET', 'POST'])
@wrap_with_use_dataset
def get_cell_type_expression():
    index_version = g.resolved_index_version
    _, b = get_scfind(index_version)

    if request.method == 'POST':
        data = request.get_json()
        cell_type = data.get('cell_type')
        gene_list = data.get('gene_list')
    else:
        cell_type = request.args.get('cell_type')
        gene_list = request.args.get('gene_list')

    if not cell_type:
        return jsonify({"error": "Missing 'cell_type'"}), 400

    if isinstance(gene_list, str):
        gene_list = [g.strip() for g in gene_list.split(',')]
    elif not gene_list or not isinstance(gene_list, list):
        return jsonify({"error": "Invalid or missing gene_list"}), 400

    adata = b.getCellTypeExpression(cell_type, gene_list)
    coo = adata.X.tocoo()
    expression_matrix = {
        "data": coo.data.tolist(),
        "row": coo.row.tolist(),
        "col": coo.col.tolist(),
        "shape": adata.X.shape,
    }
    return jsonify({"expression_matrix": expression_matrix, "var_names": adata.var_names.tolist()})

@app.route('/api/getCellTypeExpressionBinData', methods=['GET', 'POST'])
@wrap_with_use_dataset
def get_cell_type_expression_bin_data():
    index_version = g.resolved_index_version
    _, b = get_scfind(index_version)

    if request.method == 'POST':
        data = request.get_json()
        cell_type = data.get('cell_type')
        gene_list = data.get('gene_list')
        bin_length = float(data.get('bin_length', 1))
    else:
        cell_type = request.args.get('cell_type')
        gene_list = request.args.getlist('gene_list')
        bin_length = request.args.get('bin_length', default=1, type=float)

    if not cell_type or not gene_list:
        return jsonify({"error": "Missing 'cell_type' or 'gene_list'"}), 400

    if isinstance(gene_list, str):
        gene_list = [g.strip() for g in gene_list.split(',')]

    res = b.getCellTypeExpressionBinData(cell_type, gene_list, bin_length)
    return jsonify(res)

@app.route('/api/cellTypeCountForDataset', methods=['GET', 'POST'])
@wrap_with_use_dataset
def get_cellTypeCountForDataset():
    index_version = g.resolved_index_version
    a, _ = get_scfind(index_version)

    dataset = (request.get_json().get('dataset') if request.method == 'POST' else request.args.get('dataset'))
    if not dataset or not isinstance(dataset, str):
        return jsonify({"error": "Missing or invalid 'dataset'"}), 400

    result_df = a.cellTypeCountforDataset(dataset).reset_index()
    return jsonify({"cellTypeCounts": result_df.to_dict(orient='records')})

@app.route('/api/total_cells', methods=['GET', 'POST'])
@wrap_with_use_dataset
def total_cells():
    index_version = g.resolved_index_version
    a, _ = get_scfind(index_version)
    return jsonify({"total_cells": a.getTotalCells()})

@app.route('/api/total_cell_types', methods=['GET', 'POST'])
@wrap_with_use_dataset
def total_cell_types():
    index_version = g.resolved_index_version
    a, _ = get_scfind(index_version)
    return jsonify({"total_cell_types": a.getTotalCellTypes()})

@app.route('/api/listAllIndexVersions', methods=['GET'])
def list_all_index_versions():
    try:
        s3 = boto3.client('s3')
        S3_PREFIX = 'prod_index'
        keys = [f"{S3_PREFIX}/cell_type/index.bin", f"{S3_PREFIX}/dataset/index.bin"]
        all_versions = []
        for key in keys:
            response = s3.list_object_versions(Bucket=S3_BUCKET_NAME, Prefix=key)
            for v in response.get("Versions", []):
                version_id = v['VersionId']
                last_modified = v['LastModified'].isoformat()
                try:
                    tag_resp = s3.get_object_tagging(Bucket=S3_BUCKET_NAME, Key=key, VersionId=version_id)
                    tags = {t['Key']: t['Value'] for t in tag_resp.get('TagSet', [])}
                except Exception:
                    tags = {}
                all_versions.append({
                    "key": key,
                    "version_id": version_id,
                    "last_modified": last_modified,
                    "tags": tags,
                })
        return jsonify({"index_versions": all_versions})
    except Exception as e:
        return jsonify({"error": str(e)}), 500

@app.route('/api/getAllIndexVersionsConfig', methods=['GET'])
def get_all_index_versions_config():
    try:
        return jsonify(_read_versions_manifest())
    except Exception as e:
        return jsonify({"error": str(e)}), 500

@app.route('/api/testCache', methods=['GET'])
def test_cache():
    with scfind_cache_lock:
        return jsonify({"cached_versions": list(scfind_cache.keys())})
        
@app.route('/memoryStatus', methods=['GET'])
def memory_status():
    memory_stats = {
        "rss_mb": round(rss_mb(), 2),
        "cache_size": len(scfind_cache),
        "cache_max": CACHE_MAXSIZE,
        "ttl_seconds": CACHE_TTL,
    }
    app.logger.info(f"[MEMORY STATUS] {memory_stats}")
    return jsonify(memory_stats)


@app.route('/health', methods=['GET'])
def health():
    return 'ok'

if __name__ == '__main__':
    debug_mode = os.environ.get("DEBUG", "False").lower() == "true"
    port = int(os.environ.get("PORT", 8080))
    app.run(host='0.0.0.0', port=port, debug=debug_mode)
