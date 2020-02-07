from flask import Flask, request
from flask_cors import CORS

from src.analysis.analyzer import BaseAnalyzer
from src.analysis.prepare_data import utilWrapper

app = Flask(__name__)
CORS(app)

@app.route('/',methods=['GET'])
def index():
    return '<h1>Done</h1>'
@app.route('/data', methods=['POST'])
@utilWrapper
def get_data():
    request_body = request.get_json()
    response = BaseAnalyzer().process_request(request_body)
    return response


if "__main__" == __name__:
    # BaseAnalyzer().init_files() # initializing data file
    BaseAnalyzer().prepare_data() # preparing data file
    app.run(debug=True, host = '0.0.0.0', port=5000)
