# https://github.com/ShaokunAn/tmp-scfind_py/tree/main/scfind
import os
import boto3
import scfind
from flask import Flask, jsonify, request
from flask_cors import CORS

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
S3_FILE_KEY = 'index_with_datasets.bin'
LOCAL_FILE_PATH = '/app/index_with_datasets.bin'  
LOCAL_TEST_FILE_PATH = 'index_with_datasets.bin'  

S3_FILE_KEY2 = 'index_datasets_no_celltype.bin'
LOCAL_FILE_PATH2 = '/app/index_datasets_no_celltype.bin' 
LOCAL_TEST_FILE_PATH2 = 'index_datasets_no_celltype.bin' 

# Download file from S3
def download_from_s3(bucket_name, file_key, local_file_path):
    print(f"Downloading {file_key} from bucket {bucket_name} to {local_file_path}...")
    s3 = boto3.client('s3')
    s3.download_file(bucket_name, file_key, local_file_path)
# Function to determine if running locally or in production
def is_running_locally():
    # You can use environment variables, or check for specific files to distinguish local from production.
    return os.environ.get('ENV') == 'LOCAL'
# Load file depending on environment
def get_file_path():
    if is_running_locally():
        print("Running locally, using local file path.")
        return LOCAL_TEST_FILE_PATH, LOCAL_TEST_FILE_PATH2  
    else:
        print("Running in production, downloading file from S3.")
        download_from_s3(S3_BUCKET_NAME, S3_FILE_KEY, LOCAL_FILE_PATH)
        download_from_s3(S3_BUCKET_NAME, S3_FILE_KEY2, LOCAL_FILE_PATH2)
        return LOCAL_FILE_PATH, LOCAL_FILE_PATH2  

file_path1, file_path2 = get_file_path()
a = scfind.SCFind()
a.loadObject(file_path1)
b = scfind.SCFind()
b.loadObject(file_path2)

@app.route('/api/findDatasetForCellType', methods=['GET', 'POST'])
def find_dataset_for_cell_type_api():
    try:
        if request.method == 'POST':
            data = request.get_json()
            cell_type = data.get('cell_type')
        else:  # GET
            cell_type = request.args.get('cell_type')

        if not cell_type:
            return jsonify({"error": "Missing 'cell_type' parameter"}), 400

        datasets = a.find_dataset_for_cell_type(cell_type)
        return jsonify({"datasets": datasets})
    except Exception as e:
        return jsonify({"error": str(e)}), 400
    
@app.route('/api/cellTypeNames', methods=['GET', 'POST'])
def get_cellTypeNames():
    try:
        cellTypeNames_result = a.cellTypeNames()
        return jsonify({"cellTypeNames": cellTypeNames_result})
    except Exception as e:
        return jsonify({"error": str(e)}), 400

@app.route('/api/marker_genes', methods=['GET', 'POST'])
def get_marker_genes():
    try:
        marker_genes = request.args.get('marker_genes').split(',')
        dataset_name = request.args.get('dataset_name')
        if dataset_name:
            dataset_name = dataset_name.split(',')
        marker_genes_result = a.markerGenes(marker_genes, dataset_name)
        if isinstance(marker_genes_result, dict):
            return jsonify({"findGeneSignatures":marker_genes_result})
        elif hasattr(marker_genes_result, "to_dict"):
            return jsonify({"findGeneSignatures": marker_genes_result.to_dict(orient='records')})
        else:
            return jsonify({"error": "Unexpected data type returned"}), 400
    except Exception as e:
        return jsonify({"error": str(e)}), 400

@app.route('/api/cellTypeMarkers', methods=['GET', 'POST'])
def get_cellTypeMarkers():
    try:
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
        gene_list = request.args.get('gene_list').split(',')
        cell_types = request.args.get('cell_types').split(',')
        background_cell_types = request.args.get('background_cell_types')
        if background_cell_types:
            background_cell_types = background_cell_types.split(',')
        sort_field = request.args.get('sort_field', default='f1', type=str)
        include_prefix = request.args.get('include_prefix', default=True, type=bool)

        evaluateMarkers_result = a.evaluateMarkers(gene_list, cell_types, background_cell_types, sort_field, include_prefix)
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
        gene_list = request.args.get('gene_list').split(',')
        dataset_name = request.args.get('dataset_name')
        if dataset_name:
            dataset_name = dataset_name.split(',')
        include_prefix = request.args.get('include_prefix', default=True, type=bool)
        hyperQueryCellTypes_result = a.hyperQueryCellTypes(gene_list, dataset_name,include_prefix)
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
        gene_list = request.args.get('gene_list')
        if gene_list:
            gene_list = gene_list.split(',')
        
        datasets = request.args.get('datasets')
        if datasets:
            datasets = datasets.split(',')

        min_cells = request.args.get('min_cells', default=10, type=int)
        
        min_fraction = request.args.get('min_fraction', default=0.25, type=float)
        
        findCellTypeSpecificities_result = a.findCellTypeSpecificities(
            gene_list=gene_list,
            datasets=datasets,
            min_cells=min_cells,
            min_fraction=min_fraction
        )
        if isinstance(findCellTypeSpecificities_result, dict):
            return jsonify({"findGeneSignatures": findCellTypeSpecificities_result})
        elif hasattr(findCellTypeSpecificities_result, "to_dict"):
            return jsonify({"findGeneSignatures": findCellTypeSpecificities_result.to_dict(orient='records')})
        else:
            return jsonify({"error": "Unexpected data type returned"}), 400
    except Exception as e:
        return jsonify({"error": str(e)}), 400

@app.route('/api/findTissueSpecificities', methods=['GET', 'POST'])
def findTissueSpecificities():
    try:
        gene_list = request.args.get('gene_list')
        if gene_list:
            gene_list = gene_list.split(',')
        min_cells = request.args.get('min_cells', default=10, type=int)
        findTissueSpecificities_result = a.findCellTypeSpecificities(gene_list=gene_list, min_cells=min_cells)

        if isinstance(findTissueSpecificities_result, dict):
            return jsonify({"findGeneSignatures": findTissueSpecificities_result})
        elif hasattr(findTissueSpecificities_result, "to_dict"):
            return jsonify({"findGeneSignatures": findTissueSpecificities_result.to_dict(orient='records')})
        else:
            return jsonify({"error": "Unexpected data type returned"}), 400
    except Exception as e:
        return jsonify({"error": str(e)}), 400

@app.route('/api/findHouseKeepingGenes', methods=['GET', 'POST'])
def findHouseKeepingGenes():
    try:
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
        cell_types = request.args.get('cell_types')
        if cell_types:
            cell_types = cell_types.split(',')
        min_cells = request.args.get('min_cells', default=10, type=int)
        max_genes = request.args.get('max_genes', default=1000, type=int)
        max_pval=request.args.get('max_pval', default=0, type=float)
        findGeneSignatures_result = a.findGeneSignatures(cell_types,min_cells,max_genes,min_cells,max_pval)
        if isinstance(findGeneSignatures_result, dict):
            return jsonify({"findGeneSignatures": findGeneSignatures_result})
        elif hasattr(findGeneSignatures_result, "to_dict"):
            return jsonify({"findGeneSignatures": findGeneSignatures_result.to_dict(orient='records')})
        else:
            return jsonify({"error": "Unexpected data type returned"}), 400
    except Exception as e:
        return jsonify({"error": str(e)}), 400

@app.route('/api/cellTypeCountForTissue', methods=['GET', 'POST'])
def get_cellTypeCountForTissue():
    try:
        if request.method == 'POST':
            data = request.get_json()
            tissue = data.get('tissue')
        else:
            tissue = request.args.get('tissue')

        if not tissue or not isinstance(tissue, str):
            return jsonify({"error": "Missing or invalid 'tissue' parameter"}), 400

        result_df = a.cellTypeCountForTissue(tissue)
        result_df = result_df.reset_index()
        return jsonify({"cellTypeCounts": result_df.to_dict(orient='records')})
    except Exception as e:
        return jsonify({"error": str(e)}), 400


@app.route('/api/CLID2CellType', methods=['GET', 'POST'])
def get_CLID2CellType():
    try:
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
        datasets = a.getDatasets()
        return jsonify({"datasets": datasets})
    except Exception as e:
        return jsonify({"error": str(e)}), 400
    
@app.route('/api/scfindGenes', methods=['GET'])
def get_scfind_genes():
    try:
        genes = a.scfindGenes
        return jsonify({"genes": genes})
    except Exception as e:
        return jsonify({"error": str(e)}), 400

    
# second index file
@app.route('/api/findDatasets', methods=['GET', 'POST'])
def get_findDatasets():
    try:
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


@app.route('/api/cellTypeCountForDataset', methods=['GET', 'POST'])
def get_cellTypeCountForDataset():
    try:
        if request.method == 'POST':
            data = request.get_json()
            dataset = data.get('dataset')
        else:
            dataset = request.args.get('dataset')

        if not dataset or not isinstance(dataset, str):
            return jsonify({"error": "Missing or invalid 'dataset' parameter"}), 400

        result_df = a.cellTypeCountforDataset(dataset)
        result_df = result_df.reset_index() 
        

        return jsonify({"cellTypeCounts": result_df.to_dict(orient='records')})

    except Exception as e:
        return jsonify({"error": str(e)}), 400

@app.route('/api/getCellTypeExpressionBinData', methods=['GET', 'POST'])
def get_cell_type_expression_bin_data():
    try:
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

# # execution time too long
# @app.route('/api/findSimilarGenes', methods=['GET', 'POST'])
# def get_findSimilarGenes():
#     try:
#         gene_list = request.args.get('gene_list').split(',')
#         dataset_name = request.args.get('dataset_name')
#         if dataset_name:
#             dataset_name = dataset_name.split(',')
#         top_k = request.args.get('top_k', default=5, type=int)
#         findSimilarGenes_result = a.findSimilarGenes(gene_list, dataset_name,top_k)
#         findSimilarGenes_list = findSimilarGenes_result.to_dict(orient='records')
#         return jsonify({"findSimilarGenes": findSimilarGenes_list})
#     except Exception as e:
#         return jsonify({"error": str(e)}), 400

@app.route('/health', methods=['GET'])
def health():
    return 'ok'

if __name__ == '__main__':
    debug_mode = os.environ.get("DEBUG", "False").lower() == "true"
    port = int(os.environ.get("PORT", 8080)) 
    app.run(host='0.0.0.0', port=port, debug=debug_mode)
