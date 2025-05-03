import json
import requests
import logging
import os
from datetime import datetime

logging.basicConfig(level=logging.DEBUG, format='%(asctime)s - %(levelname)s - %(message)s')


def format_slack_message(message):

    json_message = {
	   "blocks": [
		{
			"type": "section",
			"text": {
				"text": ":red_circle: *pipeline failure:* Snowflake notification integration output :point_down: ",
				"type": "mrkdwn"
			}
		},
		{
			"type": "section",
			"text": {
				"text": "```" + json.dumps(message,indent=2, separators=(',', ': ')) + "```",
				"type": "mrkdwn"
			}
		}
	]
    }

    return json_message

def lambda_handler(event, context):
    if event:

        message = format_slack_message(event)
        webhook_uri = os.environ['slack_channel_url']

        if message:
            logging.info('Starting sending message to slack')
            response = requests.post(
                webhook_uri, data=json.dumps(message),
                headers={'Content-Type': 'application/json'}
            )
            logging.info(response.text)
            logging.info('Finished sending message to Slack webhook')

            if response.status_code != 200:
                raise ValueError(
                    'Request to slack returned an error %s, the response is:\n%s'
                    % (response.status_code, response.text)
                )
        else:
            return {"status": 501, "message": "Not valid SNS message"}
