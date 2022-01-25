from collections import Counter
import csv
from concurrent.futures import ThreadPoolExecutor
import json


from app import main


header = False
lines = []


def sentiment_analysis(data):
    sentiments = data['sentiment_analysis_results']
    total = len(sentiments)
    positive = 0
    negative = 0
    neutral = 0

    for sentiment in sentiments:
        if sentiment['sentiment'] == "POSITIVE":
            positive += 1
        if sentiment['sentiment'] == "NEGATIVE":
            negative += 1
        if sentiment['sentiment'] == "NEUTRAL":
            neutral += 1

    posPercentage = str(round(((positive / total) * 100), 2)) + "%"
    negPercentage = str(round(((negative / total) * 100), 2)) + "%"
    neutPercentage = str(round(((neutral / total) * 100), 2)) + "%"
    
    return f"Total Positive Sentiments: {positive} ({posPercentage})\nTotal Negative Sentiments: {negative} ({negPercentage})\nTotal Neutral Sentiments: {neutral} ({neutPercentage})"


def entity_detection(data):
    entities = data['entities']
    entities_detected = []
    text = ''

    for entity in entities:
        entities_detected.append(entity['entity_type'])

    counts = Counter(entities_detected).most_common()

    # https://stackoverflow.com/questions/20510768/count-frequency-of-words-in-a-list-and-sort-by-frequency
    for item in counts:
        text += f"{item[0]} ( count: {item[1]} )\n"

    return text


def keyword_detection(data):
    keywords = data['auto_highlights_result']['results']
    keywords_detected = []
    text = ''

    for word in keywords:
        times = word['count']

        for x in range(0, times):
            keywords_detected.append(word['text'])

    counts = Counter(keywords_detected).most_common()

    # https://stackoverflow.com/questions/20510768/count-frequency-of-words-in-a-list-and-sort-by-frequency
    for item in counts:
        text += f"{item[0]} ( count: {item[1]} )\n"

    return text


def topic_detection(data):
    topics = data['iab_categories_result']['summary']
    text = ''

    for topic in topics.items():
        if ">" in topic[0]:
            # splits topic to only show the label after the last >
            label = topic[0].rsplit('>', 1)
            text += f"{label[1]}: {round((topic[1] * 100), 0)}\n"
        else:
            text += f"{topic[0]}: {round((topic[1] * 100), 0)}\n"
    
    return text


def content_safety_detection(data):
    contents = data['content_safety_labels']['summary']
    text = ''

    for topic in contents.items():
        text += f"{topic[0]}: {round((topic[1] * 100), 0)}\n"

    if len(text) == 0:
        text = "No content labels were triggered."
    
    return text


def run_file(audio_url, file_name):
    global header
    data = main(audio_url)
    text_filename = file_name + ".txt"
    json_filename = file_name + ".json"
    file1 = open("./text/" + text_filename, 'x')
    file2 = open("./json/" + json_filename, 'x')

    file1.write(data['text'])
    file1.close()
    file2.write(json.dumps(data))
    file2.close()

    sentiment = ""
    entities = ""
    keywords = ""
    topics = ""
    content_safety = ""

    if len(data['text']) != 0:
        sentiment = sentiment_analysis(data)
        entities = entity_detection(data)
        keywords = keyword_detection(data)
        topics = topic_detection(data)
        content_safety = content_safety_detection(data)


    with open(f'enrichment.csv', 'a', newline='') as f:
        fieldnames = ['File Name','Topic Detection', 'Keyword Detection', 'Content Safety Detection', 'Entity Detection', 'Sentiment Analysis']
        writer = csv.DictWriter(f, fieldnames=fieldnames)


        if header is False:
            writer.writeheader()
            header = True

        writer.writerow({
            'File Name': file_name,
            'Topic Detection': topics,
            'Keyword Detection': keywords,
            'Content Safety Detection': content_safety,
            'Entity Detection': entities,
            'Sentiment Analysis': sentiment
        })


f = open("urls.txt", "r")
# loop over each line in text file and append them to dictionary
for x in f:
    lines.append(x)

with ThreadPoolExecutor(5) as executor:
    for line in lines:
        # splits file names and urls
        split_line = line.split(",")
        split_url = split_line[1].split("\n")
        executor.submit(run_file(split_url[0], split_line[0]))




