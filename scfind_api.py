# https://github.com/ShaokunAn/tmp-scfind_py/tree/main/scfind
from flask import Flask, jsonify, request
from flask_cors import CORS
import os
import json
import boto3
import scfind
from botocore.exceptions import ClientError
from threading import Lock

app = Flask(__name__)
# Enable CORS for specific ports
CORS(app, resources={
    r"/api/*": {
        "origins": [
            "http://localhost:5001",                     # local dev
            "http://localhost:6006",                     # local dev
            "https://scfind.dev.hubmapconsortium.org",   # staging/prod
            "https://scfind.hubmapconsortium.org",        # official prod
            "https://portal.dev.hubmapconsortium.org",     # portal dev
            "https://portal.test.hubmapconsortium.org",     # portal test
            "https://portal-prod.test.hubmapconsortium.org", # portal prod
            "https://portal.hubmapconsortium.org" # portal prod
        ]
    }
}, supports_credentials=True)

# S3 configuration
S3_BUCKET_NAME = 'scfind-dataset'
scfind_cache = {}
scfind_cache_lock = Lock()

def get_file_path(index_version):
    s3 = boto3.client('s3')
    try:
        response = s3.get_object(Bucket=S3_BUCKET_NAME, Key='prod_index/all_index_versions.json')
        content = response['Body'].read().decode('utf-8')
        index_versions = json.loads(content)
        version_metadata = index_versions['versions'][index_version or index_versions['latest']]
    except Exception as e:
        raise RuntimeError(f"Failed to fetch version metadata: {e}")

    file1_info = version_metadata["index_with_datasets.bin"]
    file2_info = version_metadata["index_datasets_no_celltype.bin"]

    file_key1 = file1_info["key"]
    version_id1 = file1_info["version_id"]

    file_key2 = file2_info["key"]
    version_id2 = file2_info["version_id"]

    local_file_path1 = f"/app/{index_version}_index_with_datasets.bin"
    local_file_path2 = f"/app/{index_version}_index_datasets_no_celltype.bin"

    if not os.path.exists(local_file_path1):
        print(f"Downloading {file_key1} to {local_file_path1}")
        s3.download_file(S3_BUCKET_NAME, file_key1, local_file_path1, ExtraArgs={'VersionId': version_id1})

    if not os.path.exists(local_file_path2):
        print(f"Downloading {file_key2} to {local_file_path2}")
        s3.download_file(S3_BUCKET_NAME, file_key2, local_file_path2, ExtraArgs={'VersionId': version_id2})

    return local_file_path1, local_file_path2

def get_scfind(index_version=None):
    if index_version is None:
        s3 = boto3.client('s3')
        response = s3.get_object(Bucket=S3_BUCKET_NAME, Key='prod_index/all_index_versions.json')
        content = response['Body'].read().decode('utf-8')
        index_versions = json.loads(content)
        index_version = index_versions['latest']

    with scfind_cache_lock:
        if index_version in scfind_cache:
            return scfind_cache[index_version]

        path1, path2 = get_file_path(index_version)
        a = scfind.SCFind()
        a.loadObject(path1)
        b = scfind.SCFind()
        b.loadObject(path2)
        scfind_cache[index_version] = (a, b)
        return a, b

@app.route('/api/findDatasetForCellType', methods=['GET', 'POST'])
def find_dataset_for_cell_type_api():
    try:
        index_version = request.args.get('index_version')
        a, _ = get_scfind(index_version)

        cell_type = request.get_json().get('cell_type') if request.method == 'POST' else request.args.get('cell_type')
        if not cell_type:
            return jsonify({"error": "Missing 'cell_type' parameter"}), 400

        datasets = a.find_dataset_for_cell_type(cell_type)
        return jsonify({"datasets": datasets})
    except Exception as e:
        return jsonify({"error": str(e)}), 400
    
@app.route('/api/cellTypeNames', methods=['GET', 'POST'])
def get_cellTypeNames():
    try:
        index_version = request.args.get('index_version')
        a, _ = get_scfind(index_version)
        cellTypeNames_result = a.cellTypeNames()
        return jsonify({"cellTypeNames": cellTypeNames_result})
    except Exception as e:
        return jsonify({"error": str(e)}), 400
    
@app.route('/api/marker_genes', methods=['GET', 'POST'])
def get_marker_genes():
    try:
        index_version = request.args.get('index_version')
        a, _ = get_scfind(index_version)

        marker_genes = request.args.get('marker_genes').split(',')
        dataset_name = request.args.get('dataset_name')
        if dataset_name:
            dataset_name = dataset_name.split(',')
        marker_genes_result = a.markerGenes(marker_genes, dataset_name)
        if isinstance(marker_genes_result, dict):
            return jsonify({"findGeneSignatures": marker_genes_result})
        elif hasattr(marker_genes_result, "to_dict"):
            return jsonify({"findGeneSignatures": marker_genes_result.to_dict(orient='records')})
        else:
            return jsonify({"error": "Unexpected data type returned"}), 400
    except Exception as e:
        return jsonify({"error": str(e)}), 400

@app.route('/api/cellTypeMarkers', methods=['GET', 'POST'])
def get_cellTypeMarkers():
    try:
        index_version = request.args.get('index_version')
        a, _ = get_scfind(index_version)

        cell_types = request.args.get('cell_types')
        if cell_types:
            cell_types = cell_types.split(',')

        background_cell_types = request.args.get('background_cell_types')
        if background_cell_types:
            background_cell_types = background_cell_types.split(',')

        top_k = request.args.get('top_k', default=5, type=int)
        sort_field = request.args.get('sort_field', default='f1', type=str)
        include_prefix = request.args.get('include_prefix', default=True, type=bool)

        cellTypeMarkers_result = a.cellTypeMarkers(
            cell_types=cell_types,
            background_cell_types=background_cell_types,
            top_k=top_k,
            sort_field=sort_field,
            include_prefix=include_prefix
        )
        if isinstance(cellTypeMarkers_result, dict):
            return jsonify({"findGeneSignatures": cellTypeMarkers_result})
        elif hasattr(cellTypeMarkers_result, "to_dict"):
            return jsonify({"findGeneSignatures": cellTypeMarkers_result.to_dict(orient='records')})
        else:
            return jsonify({"error": "Unexpected data type returned"}), 400
    except Exception as e:
        return jsonify({"error": str(e)}), 400

@app.route('/api/evaluateMarkers', methods=['GET', 'POST'])
def get_evaluateMarkers():
    try:
        index_version = request.args.get('index_version')
        a, _ = get_scfind(index_version)

        gene_list = request.args.get('gene_list').split(',')
        cell_types = request.args.get('cell_types').split(',')
        background_cell_types = request.args.get('background_cell_types')
        if background_cell_types:
            background_cell_types = background_cell_types.split(',')
        sort_field = request.args.get('sort_field', default='f1', type=str)
        include_prefix = request.args.get('include_prefix', default=True, type=bool)

        evaluateMarkers_result = a.evaluateMarkers(
            gene_list, cell_types, background_cell_types, sort_field, include_prefix
        )
        if isinstance(evaluateMarkers_result, dict):
            return jsonify({"findGeneSignatures": evaluateMarkers_result})
        elif hasattr(evaluateMarkers_result, "to_dict"):
            return jsonify({"findGeneSignatures": evaluateMarkers_result.to_dict(orient='records')})
        else:
            return jsonify({"error": "Unexpected data type returned"}), 400
    except Exception as e:
        return jsonify({"error": str(e)}), 400


@app.route('/api/hyperQueryCellTypes', methods=['GET', 'POST'])
def get_hyperQueryCellTypes():
    try:
        index_version = request.args.get('index_version')
        a, _ = get_scfind(index_version)

        gene_list = request.args.get('gene_list').split(',')
        dataset_name = request.args.get('dataset_name')
        if dataset_name:
            dataset_name = dataset_name.split(',')

        include_prefix = request.args.get('include_prefix', default=True, type=bool)
        hyperQueryCellTypes_result = a.hyperQueryCellTypes(gene_list, dataset_name, include_prefix)

        if isinstance(hyperQueryCellTypes_result, dict):
            return jsonify({"findGeneSignatures": hyperQueryCellTypes_result})
        elif hasattr(hyperQueryCellTypes_result, "to_dict"):
            return jsonify({"findGeneSignatures": hyperQueryCellTypes_result.to_dict(orient='records')})
        else:
            return jsonify({"error": "Unexpected data type returned"}), 400
    except Exception as e:
        return jsonify({"error": str(e)}), 400


@app.route('/api/findCellTypeSpecificities', methods=['GET', 'POST'])
def findCellTypeSpecificities():
    try:
        index_version = request.args.get('index_version')
        a, _ = get_scfind(index_version)

        gene_list = request.args.get('gene_list')
        if gene_list:
            gene_list = gene_list.split(',')

        datasets = request.args.get('datasets')
        if datasets:
            datasets = datasets.split(',')

        min_cells = request.args.get('min_cells', default=10, type=int)
        min_fraction = request.args.get('min_fraction', default=0.25, type=float)

        result = a.findCellTypeSpecificities(
            gene_list=gene_list,
            datasets=datasets,
            min_cells=min_cells,
            min_fraction=min_fraction
        )
        if isinstance(result, dict):
            return jsonify({"findGeneSignatures": result})
        elif hasattr(result, "to_dict"):
            return jsonify({"findGeneSignatures": result.to_dict(orient='records')})
        else:
            return jsonify({"error": "Unexpected data type returned"}), 400
    except Exception as e:
        return jsonify({"error": str(e)}), 400


@app.route('/api/findTissueSpecificities', methods=['GET', 'POST'])
def findTissueSpecificities():
    try:
        index_version = request.args.get('index_version')
        a, _ = get_scfind(index_version)

        gene_list = request.args.get('gene_list')
        if gene_list:
            gene_list = gene_list.split(',')

        min_cells = request.args.get('min_cells', default=10, type=int)

        result = a.findCellTypeSpecificities(gene_list=gene_list, min_cells=min_cells)
        if isinstance(result, dict):
            return jsonify({"findGeneSignatures": result})
        elif hasattr(result, "to_dict"):
            return jsonify({"findGeneSignatures": result.to_dict(orient='records')})
        else:
            return jsonify({"error": "Unexpected data type returned"}), 400
    except Exception as e:
        return jsonify({"error": str(e)}), 400

@app.route('/api/findHouseKeepingGenes', methods=['GET', 'POST'])
def findHouseKeepingGenes():
    try:
        index_version = request.args.get('index_version')
        a, _ = get_scfind(index_version)
        cell_types = request.args.get('cell_types').split(',')
        min_recall = request.args.get('min_recall', default=0.5, type=float)
        max_genes = request.args.get('max_genes', default=1000, type=int)
        findHouseKeepingGenes_result = a.findHouseKeepingGenes(cell_types,min_recall,max_genes)
        if isinstance(findHouseKeepingGenes_result, dict):
            return jsonify({"findGeneSignatures": findHouseKeepingGenes_result})
        elif hasattr(findHouseKeepingGenes_result, "to_dict"):
            return jsonify({"findGeneSignatures": findHouseKeepingGenes_result.to_dict(orient='records')})
        elif isinstance(findHouseKeepingGenes_result, list):  
            return jsonify({"findGeneSignatures": findHouseKeepingGenes_result})
        elif isinstance(findHouseKeepingGenes_result, str):  
            return jsonify({"findGeneSignatures": {"message": findHouseKeepingGenes_result}})
        else:
            return jsonify({"error": f"Unexpected data type returned: {type(findHouseKeepingGenes_result)}"}), 400
    except Exception as e:
        return jsonify({"error": str(e)}), 400

@app.route('/api/findGeneSignatures', methods=['GET', 'POST'])
def get_findGeneSignatures():
    try:
        index_version = request.args.get('index_version')
        a, _ = get_scfind(index_version)

        cell_types = request.args.get('cell_types')
        if cell_types:
            cell_types = cell_types.split(',')

        min_cells = request.args.get('min_cells', default=10, type=int)
        max_genes = request.args.get('max_genes', default=1000, type=int)
        max_pval = request.args.get('max_pval', default=0, type=float)

        result = a.findGeneSignatures(cell_types, min_cells, max_genes, min_cells, max_pval)
        if isinstance(result, dict):
            return jsonify({"findGeneSignatures": result})
        elif hasattr(result, "to_dict"):
            return jsonify({"findGeneSignatures": result.to_dict(orient='records')})
        else:
            return jsonify({"error": "Unexpected data type returned"}), 400
    except Exception as e:
        return jsonify({"error": str(e)}), 400


@app.route('/api/cellTypeCountForTissue', methods=['GET', 'POST'])
def get_cellTypeCountForTissue():
    try:
        index_version = request.args.get('index_version')
        a, _ = get_scfind(index_version)

        if request.method == 'POST':
            data = request.get_json()
            tissue = data.get('tissue')
        else:
            tissue = request.args.get('tissue')

        if not tissue or not isinstance(tissue, str):
            return jsonify({"error": "Missing or invalid 'tissue' parameter"}), 400

        result_df = a.cellTypeCountForTissue(tissue).reset_index()
        return jsonify({"cellTypeCounts": result_df.to_dict(orient='records')})
    except Exception as e:
        return jsonify({"error": str(e)}), 400



@app.route('/api/CLID2CellType', methods=['GET', 'POST'])
def get_CLID2CellType():
    try:
        index_version = request.args.get('index_version')
        a, _ = get_scfind(index_version)

        if request.method == 'POST':
            data = request.get_json()
            clid_label = data.get('CLID_label')
        else:
            clid_label = request.args.get('CLID_label')

        if not clid_label:
            return jsonify({"error": "Missing 'CLID_label' parameter"}), 400

        result = a.CLID2CellType(clid_label)
        return jsonify({"cell_types": list(result)})
    except Exception as e:
        return jsonify({"error": str(e)}), 400



@app.route('/api/CellType2CLID', methods=['GET', 'POST'])
def get_CellType2CLID():
    try:
        index_version = request.args.get('index_version')
        a, _ = get_scfind(index_version)

        if request.method == 'POST':
            data = request.get_json()
            cell_type = data.get('cell_type')
        else:
            cell_type = request.args.get('cell_type')

        if not cell_type:
            return jsonify({"error": "Missing 'cell_type' parameter"}), 400

        result = a.CellType2CLID(cell_type)
        return jsonify({"CLIDs": list(result)})
    except Exception as e:
        return jsonify({"error": str(e)}), 400

@app.route('/api/getDatasets', methods=['GET'])
def get_datasets():
    try:
        index_version = request.args.get('index_version')
        a, _ = get_scfind(index_version)

        datasets = a.getDatasets()
        return jsonify({"datasets": datasets})
    except Exception as e:
        return jsonify({"error": str(e)}), 400

    
@app.route('/api/scfindGenes', methods=['GET'])
def get_scfind_genes():
    try:
        index_version = request.args.get('index_version')
        a, _ = get_scfind(index_version)

        genes = a.scfindGenes
        return jsonify({"genes": genes})
    except Exception as e:
        return jsonify({"error": str(e)}), 400
    
# second index file
@app.route('/api/findDatasets', methods=['GET', 'POST'])
def get_findDatasets():
    try:
        index_version = request.args.get('index_version')
        _, b = get_scfind(index_version)

        if request.method == 'POST':
            data = request.get_json()
            gene_list = data.get('gene_list')
            datasets = data.get('datasets')
            min_cells = data.get('min_cells', 10)
        else:
            gene_list = request.args.get('gene_list')
            datasets = request.args.get('datasets')
            min_cells = request.args.get('min_cells', default=10, type=int)

        if isinstance(gene_list, str):
            gene_list = gene_list.split(',')
        if isinstance(datasets, str):
            datasets = datasets.split(',')

        result = b.findDatasets(gene_list=gene_list, min_cells=min_cells, datasets=datasets)
        return jsonify({"findDatasets": result})
    except Exception as e:
        return jsonify({"error": str(e)}), 400

@app.route('/api/getCellTypeExpression', methods=['GET', 'POST'])
def get_cell_type_expression():
    try:
        index_version = request.args.get('index_version')
        _, b = get_scfind(index_version)

        if request.method == 'POST':
            data = request.get_json()
            cell_type = data.get('cell_type')
            gene_list = data.get('gene_list')
        else:
            cell_type = request.args.get('cell_type')
            gene_list = request.args.get('gene_list')

        if not cell_type:
            return jsonify({"error": "Missing 'cell_type' parameter"}), 400

        if isinstance(gene_list, str):
            gene_list = [g.strip() for g in gene_list.split(',')]
        elif gene_list is None:
            gene_list = []

        adata = b.getCellTypeExpression(cell_type, gene_list)
        coo = adata.X.tocoo()

        expression_matrix = {
            "data": coo.data.tolist(),
            "row": coo.row.tolist(),
            "col": coo.col.tolist(),
            "shape": adata.X.shape
        }

        return jsonify({
            "expression_matrix": expression_matrix,
            "var_names": adata.var_names.tolist()
        })
    except Exception as e:
        return jsonify({"error": str(e)}), 400

@app.route('/api/getCellTypeExpressionBinData', methods=['GET', 'POST'])
def get_cell_type_expression_bin_data():
    try:
        index_version = request.args.get('index_version')
        _, b = get_scfind(index_version)

        if request.method == 'POST':
            data = request.get_json()
            cell_type = data.get('cell_type')
            gene_list = data.get('gene_list')
            bin_length = data.get('bin_length', default=1, type=float)
        else:
            cell_type = request.args.get('cell_type')
            gene_list = request.args.getlist('gene_list')
            bin_length = request.args.get('bin_length', default=1, type=float)

        if not cell_type or not gene_list:
            return jsonify({"error": "Missing 'cell_type' or 'gene_list'"}), 400

        if isinstance(gene_list, str):
            gene_list = [g.strip() for g in gene_list.split(',')]

        result = b.getCellTypeExpressionBinData(cell_type, gene_list, bin_length)
        return jsonify(result)
    except Exception as e:
        return jsonify({"error": str(e)}), 400

@app.route('/api/cellTypeCountForDataset', methods=['GET', 'POST'])
def get_cellTypeCountForDataset():
    try:
        index_version = request.args.get('index_version')
        a, _ = get_scfind(index_version)

        if request.method == 'POST':
            data = request.get_json()
            dataset = data.get('dataset')
        else:
            dataset = request.args.get('dataset')

        if not dataset or not isinstance(dataset, str):
            return jsonify({"error": "Missing or invalid 'dataset' parameter"}), 400

        result_df = a.cellTypeCountforDataset(dataset).reset_index()
        return jsonify({"cellTypeCounts": result_df.to_dict(orient='records')})
    except Exception as e:
        return jsonify({"error": str(e)}), 400

    
@app.route('/api/total_cells', methods=['GET'])
def total_cells():
    """
    Return total number of cells in the cell-type index (from object a).
    """
    try:
        index_version = request.args.get('index_version')
        a, _ = get_scfind(index_version)

        n = a.getTotalCells()
        return jsonify({"total_cells": n})
    except Exception as e:
        return jsonify({"error": str(e)}), 500

@app.route('/api/total_cell_types', methods=['GET'])
def total_cell_types():
    """
    Return total number of cell types in the cell-type index (from object a).
    """
    try:
        index_version = request.args.get('index_version')
        a, _ = get_scfind(index_version)

        n = a.getTotalCellTypes()
        return jsonify({"total_cell_types": n})
    except Exception as e:
        return jsonify({"error": str(e)}), 500

@app.route('/api/listIndexVersions', methods=['GET'])
def list_index_versions():
    try:
        s3 = boto3.client('s3')
        key = 'prod_index/all_index_versions.json'
        response = s3.get_object(Bucket=S3_BUCKET_NAME, Key=key)
        content = response['Body'].read().decode('utf-8')
        index_versions = json.loads(content)
        return jsonify({"index_versions": index_versions})
    except ClientError as e:
        return jsonify({"error": f"S3 error: {str(e)}"}), 500
    except Exception as e:
        return jsonify({"error": str(e)}), 500

@app.route('/api/testCache', methods=['GET'])
def test_cache():
    return jsonify({"cached_versions": list(scfind_cache.keys())})

@app.route('/health', methods=['GET'])
def health():
    return 'ok'

if __name__ == '__main__':
    debug_mode = os.environ.get("DEBUG", "False").lower() == "true"
    port = int(os.environ.get("PORT", 8080))  # Default to 80 if PORT is not set
    app.run(host='0.0.0.0', port=port, debug=debug_mode)
