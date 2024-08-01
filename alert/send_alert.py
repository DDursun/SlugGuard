import os
from pathlib import Path
from dotenv import load_dotenv
import requests
from slack_sdk import WebClient

env_path = Path('alert') / '.env'
load_dotenv(dotenv_path=env_path)
slack_token = os.environ['SLACK_TOKEN']
channel_id = "C07ASV6236J"


def send_message(text):
    client = WebClient(token=os.environ['SLACK_TOKEN'])
    client.chat_postMessage(channel="#slugging-wells", text = text)

def send_image(well):

    ## Send image to a channel
    # Get an image in your environment and transform this in bytes
    img = open(f"{well}.jpg", 'rb').read()
    # Authenticate to the Slack API via the generated token
    client = WebClient(token=os.environ['SLACK_TOKEN'])
  

    # Step 1/4: Get the URL to upload to.
    attachment_size = len(img)
    print(attachment_size)

    url_for_uploading = client.files_getUploadURLExternal(
        token=slack_token,
        filename="Well-01.jpg",
        length=attachment_size,
    )
    
    if url_for_uploading["ok"]:
        for item in url_for_uploading:
            print(f"{item}")
    else:
        raise ValueError(
            f"Failed to get the URL for uploading the attachment to Slack! Response: {url_for_uploading}"
        )
    
    # Step 2/4: Upload the file to the URL.
    payload = {
        "filename": "Well-01.jpg",
        "token": slack_token,
        # "channels": ["#dsc-dbx-alerts-test"],
    }
    response = requests.post(
        url_for_uploading["upload_url"], params=payload, data=img
    )
    
    if response.status_code == 200:
        print(
            f"Response from Slack: {response.status_code}, {response.text}"
        )
    else:
        raise ValueError(
            f"Response from Slack: {response.status_code}, {response.text}, {response.headers}"
        )
    
    file_id = url_for_uploading["file_id"]
    
    # Step 3/4: Make the file accessible in the channel.
    client.files_completeUploadExternal(
        token=slack_token,
        files=[{"id": file_id, "title": "Attachment"}],
        channel_id=channel_id,
        initial_comment=None,
        thread_ts=None,
    )
    
    attachment_with_slack_url = {
        "title": "Attachment",
        "image_url": url_for_uploading["upload_url"],
    }
    
    # Step 4/4: Send the message to the specified Slack channel.
    response = client.chat_postMessage(
        channel=channel_id,
        text="message",
        attachments=[attachment_with_slack_url],
    )
    
    # Check if message sending was successful.
    if response.status_code != 200:
        raise ValueError(
            f"Failed to send the message to Slack! Status code returned from the Slack API: {response.status_code}"
        )
    