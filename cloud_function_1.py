import functions_framework
import json
import base64
from google.cloud import storage, pubsub_v1
import random
import requests
import psycopg2


@functions_framework.http
def readUserData(request):
    request_json = request.get_json(silent=True)
    
    if not request_json or 'file_urls' not in request_json:
        return 'No file URLs provided', 400

    file_urls = request_json['file_urls']
    if not isinstance(file_urls, list):
        return 'Invalid file_urls format', 400

    publisher = pubsub_v1.PublisherClient()
    topic_name = 'projects/deep-hook-419115/topics/TP_commercial_to_risk'

    vision_api_key = os.getenv("VISION_API_KEY")
    vision_api_url = 'https://vision.googleapis.com/v1/images:annotate?key=' + vision_api_key

    storage_client = storage.Client()
    text_results = []
    client_last_name = ""
    client_first_name = ""
    client_balance = ""
    for file_url in file_urls:
        # Loop over Cloud Storage new files
        bucket_name, blob_name = parse_gcs_url(file_url)
        bucket = storage_client.bucket(bucket_name)
        blob = bucket.blob(blob_name)
        if file_url.endswith(('.jpg', '.jpeg', '.png')):
            # If the file is a document, we download it and then send it to the Vision API (For OCR)
            image_content = blob.download_as_bytes()

            payload = {
                "requests": [{
                    "image": {
                        "content": base64.b64encode(image_content).decode('UTF-8')
                    },
                    "features": [{
                        "type": "TEXT_DETECTION"
                    }]
                }]
            }

            response = requests.post(vision_api_url, json=payload)
            result = response.json()
            # we read the OCR Response
            if 'textAnnotations' in result['responses'][0]:
                text_results.append(result['responses'][0]['textAnnotations'][0]['description'])
            else:
                text_results.append("No text found")
            image_url = file_url

        elif file_url.endswith('.json'):
            # Assuming this is the JSON file with client data, we simply take the fields we need
            json_content = blob.download_as_text()
            data = json.loads(json_content)
            if 'result' in data and len(data['result']) > 0:
                client = data['result'][0]
                client_first_name = client['name']['first']
                client_last_name = client['name']['last']
                # This calls the Cloud SQL DB and retrieves user data
                client_balance = get_user_account_data(client_first_name, client_last_name)
    # Simulate score calculation
    credit_score = calculate_credit_score()

    result = {
        "extracted_texts": text_results,
        "credit_score": credit_score,
        "client_first_name": client_first_name,
        "client_last_name": client_last_name,
        "client_balance": client_balance,
        "doc_image_url": image_url
    }
    message = json.dumps(result)
    
    # Publish message to the adequate topic
    publisher.publish(topic_name, message.encode("utf-8"))

    return f"Message published to {topic_name} {message}"

def get_user_account_data(first_name, last_name):
    # Setup database connection
    try:
        conn = psycopg2.connect(
            host=os.getenv("CLOUD_SQL_HOST"), 
            dbname='postgres',
            user='postgres',
            password= os.getenv("CLOUD_SQL_PASSWORD")
        )
        cursor = conn.cursor()
    except Exception as e:
        print("Unable to connect to the database:", e)
        return f"Database connection failed: {str(e)}", 500

    
     # SQL Query to retrieve balance
    try:
        cursor.execute("SELECT balance FROM clients WHERE first_name = %s AND last_name = %s", (first_name, last_name))
        balance = cursor.fetchone()
        if balance:
            client_balance = balance[0]  # Assuming 'balance' is a column in your table
        else:
            client_balance = 'No balance found'
    except Exception as e:
        print("Error querying the database:", e)
        client_balance = 'Query failed'

    # Ensure to close the database connection
    cursor.close()
    conn.close()
    
    return client_balance

def calculate_credit_score():
    return random.randint(0, 10)


def parse_gcs_url(gcs_url):
    """Helper function to parse GCS URLs into bucket and blob names."""
    if not gcs_url.startswith('gs://'):
        raise ValueError("URL must start with gs://")
    parts = gcs_url[len('gs://'):].split('/')
    bucket_name = parts[0]
    blob_name = '/'.join(parts[1:])
    return bucket_name, blob_name
