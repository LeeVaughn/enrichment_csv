import sys
import time
import requests


base_endpoint = "https://api.assemblyai.com/v2"
headers = {'authorization': "enter your api key here"}


# Start the transcription process
def start_transcript(audio_url):

    post_json = {
        "audio_url": audio_url,
        "auto_highlights": True,
        "iab_categories": True,
        "entity_detection": True,
        "sentiment_analysis": True,
        "content_safety": True,
    }

    r = requests.post(base_endpoint + "/transcript", headers=headers, json=post_json)
    return r.json()


# Get the completed transcription
def get_transcript(id):
    r = requests.get(base_endpoint + "/transcript/" + id, headers=headers)
    return r.json()


# Wait for the status of the transcription to be completed
def wait_for_result(id):
    print("polling for result...")
    response = get_transcript(id)
    while response['status'] not in ['completed', 'error']:
        time.sleep(3)
        response = get_transcript(response['id'])
        print(response['status'])
    return response


# Run the entire program to detect sponsorships/advertisements
def main(audio_url):
    response = start_transcript(audio_url)
    print("transcript id: %s" % response['id'])
    response = wait_for_result(response['id'])

    if response['status'] == "error":
        raise Exception(response['error'])

    return response
 

if __name__ == "__main__":
    audio_url = sys.argv[1]
    print(audio_url)
    main(audio_url)
