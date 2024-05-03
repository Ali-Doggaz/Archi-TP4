import base64
import json
import functions_framework
from google.cloud import vision
from google.oauth2 import service_account
import googleapiclient.discovery
from email.mime.text import MIMEText
import requests

# Function triggered by Pub/Sub
@functions_framework.cloud_event
def hello_pubsub(cloud_event):
    pubsub_message = json.loads(cloud_event.data['message']['data'])
    print("Received message:", pubsub_message)
    
    # Process the image using Vision API (OCR)
    extracted_text = process_image_with_vision(pubsub_message['doc_image_url'])

    # Notifies the customer
    email_content = f"Extracted Texts: {extracted_text}\nCredit Score: {pubsub_message['credit_score']}\nClient: {pubsub_message['client_first_name']} {pubsub_message['client_last_name']}\nBalance: {pubsub_message['client_balance']}"

    send_email_via_gmail("your-email@gmail.com", "Vision API and Pub/Sub Results", email_content)



def process_image_with_vision(image_url):
    # Initialize the Google Cloud Storage client
    storage_client = storage.Client()
    bucket_name, blob_name = parse_gcs_url(image_url)
    bucket = storage_client.bucket(bucket_name)
    blob = bucket.blob(blob_name)
    image_content = blob.download_as_bytes()

    # Prepare the request payload for the Vision API
    vision_api_key = os.getenv("VISION_API_KEY")
    vision_api_url = 'https://vision.googleapis.com/v1/images:annotate?key=' + vision_api_key
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

    # Send the request to the Vision API
    response = requests.post(vision_api_url, json=payload)
    result = response.json()
    if 'textAnnotations' in result['responses'][0]:
        return result['responses'][0]['textAnnotations'][0]['description']
    else:
        return "No text found"

def parse_gcs_url(gcs_url):
    """Helper function to parse GCS URLs into bucket and blob names."""
    if not gcs_url.startswith('gs://'):
        raise ValueError("URL must start with gs://")
    parts = gcs_url[len('gs://'):].split('/')
    bucket_name = parts[0]
    blob_name = '/'.join(parts[1:])
    return bucket_name, blob_name

def send_email_via_gmail(to_email, subject, content):
    # Retrieve credentials and build the Gmail service
    creds = get_credentials()
    service = googleapiclient.discovery.build('gmail', 'v1', credentials=creds)

    # Create and send the email message
    message = create_message("me", to_email, subject, content)
    send_message(service, "me", message)

def create_message(sender, to, subject, message_text):
    message = MIMEText(message_text)
    message['to'] = to
    message['from'] = sender
    message['subject'] = subject
    
    return {'raw': base64.urlsafe_b64encode(message.as_bytes()).decode()}

def send_message(service, user_id, message):
    try:
        message = (service.users().messages().send(userId=user_id, body=message).execute())
        print('Message Id: %s' % message['id'])
        return message
    except Exception as e:
        print('An error occurred: %s' % e)
        return None


def get_credentials():
    creds = credentials.Credentials(
        None,
        refresh_token=REFRESH_TOKEN,
        token_uri='https://oauth2.googleapis.com/token',
        client_id=os.getenv("CLIENT_ID"),
        client_secret=os.getenv("CLIENT_SECRET")
    )
    # Explicitly refresh the token
    request = Request()
    creds.refresh(request)
    return creds
