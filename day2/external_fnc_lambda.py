import json
import boto3

translate = boto3.client('translate')

def lambda_handler(event, context):

    # 200 is the HTTP status code for "ok".
    status_code = 200

    # The return value will contain an array of arrays (one inner array per input row).
    translated = [ ]

    try:
        # From the input parameter named "event", get the body, which contains
        # the input rows.
        event_body = event["body"]

        # Convert the input from a JSON string into a JSON object.
        payload = json.loads(event_body)

        # This is basically an array of arrays. The inner array contains the
        # row number, and a value for each parameter passed to the function.
        rows = payload["data"]

        # For each input row in the JSON object...
        for row in rows:
            translated_text = translate.translate_text(
            Text = row[1],
            SourceLanguageCode = 'en',
            TargetLanguageCode = 'it')["TranslatedText"]
            translated.append([row[0], translated_text])

        json_compatible_string_to_return = json.dumps({"data" : translated})

    except Exception as err:
        # 400 implies some type of error.
        status_code = 400
        # Tell caller what this function could not handle.
        json_compatible_string_to_return = event_body

    # Return the return value and HTTP status code.
    return {
        'statusCode': status_code,
        'body': json_compatible_string_to_return
    }
