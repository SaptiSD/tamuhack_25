from flask import Flask, jsonify, request
import boto3
import json
from botocore.exceptions import ClientError

# Initialize Flask app
app = Flask(__name__)

# AWS Configuration
AWS_REGION = "us-east-1"  # Change as per your setup
DYNAMODB_TABLE = "your-dynamodb-table-name"
LAMBDA_FUNCTION_NAME = "your-lambda-function-name"

# Initialize AWS clients
dynamodb_client = boto3.resource('dynamodb', region_name=AWS_REGION)
lambda_client = boto3.client('lambda', region_name=AWS_REGION)

# Get DynamoDB table reference
table = dynamodb_client.Table(DYNAMODB_TABLE)

@app.route('/upload-data', methods=['POST'])
def upload_data():
    """Upload user-specific JSON data to DynamoDB."""
    try:
        data = request.json
        if not data:
            return jsonify({"error": "No data provided"}), 400

        # Assuming the incoming JSON has a primary key field 'id'
        table.put_item(Item=data)
        return jsonify({"message": "Data uploaded to DynamoDB successfully"}), 200
    except Exception as e:
        return jsonify({"error": str(e)}), 500

@app.route('/predict', methods=['POST'])
def predict():
    """Send user inputs to AWS Lambda for prediction."""
    user_input = request.json
    if not user_input:
        return jsonify({"error": "No input provided"}), 400

    try:
        # Invoke AWS Lambda function
        response = lambda_client.invoke(
            FunctionName=LAMBDA_FUNCTION_NAME,
            InvocationType='RequestResponse',
            Payload=json.dumps(user_input)
        )

        # Parse the response payload
        result = json.loads(response['Payload'].read())
        return jsonify({"prediction": result}), 200
    except ClientError as e:
        return jsonify({"error": str(e)}), 500

@app.route('/get-local-data', methods=['GET'])
def get_local_data():
    """Fetch local energy/water data from DynamoDB."""
    try:
        # Scan the DynamoDB table to fetch all data
        response = table.scan()
        items = response.get('Items', [])
        return jsonify({"data": items}), 200
    except ClientError as e:
        return jsonify({"error": str(e)}), 500

@app.route('/compare-data', methods=['POST'])
def compare_data():
    """Compare user data with local data."""
    user_data = request.json
    if not user_data:
        return jsonify({"error": "No user data provided"}), 400

    try:
        # Fetch all local data from DynamoDB
        response = table.scan()
        local_data = response.get('Items', [])

        # Example comparison logic
        comparison_results = []
        for local_item in local_data:
            comparison = {}
            for metric, value in user_data.items():
                local_value = local_item.get(metric, None)
                if local_value:
                    difference = value - float(local_value)
                    comparison[metric] = {"user": value, "local": local_value, "difference": difference}
            comparison_results.append(comparison)

        return jsonify({"comparison_results": comparison_results}), 200
    except ClientError as e:
        return jsonify({"error": str(e)}), 500

if __name__ == "__main__":
    app.run(host='0.0.0.0', port=5000)
