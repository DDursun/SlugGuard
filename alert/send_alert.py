import slack
import os
from pathlib import Path
from dotenv import load_dotenv

env_path = Path('alert') / '.env'
load_dotenv(dotenv_path=env_path)



def send_message(text):
    client = slack.WebClient(token=os.environ['SLACK_TOKEN'])
    client.chat_postMessage(channel="#slugging-wells", text = text)

