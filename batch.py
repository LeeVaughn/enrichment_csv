from collections import Counter
import csv
from concurrent.futures import ThreadPoolExecutor
import json
import datetime


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

def chapter_detection(data):
    if (data.get('content_safety') == True):
        chapters = data['chapters']
        text = ""

        for chapter in chapters:
            time = datetime.timedelta(milliseconds=int(chapter['start']))
            time = (str(time).split(".")[0])
            text += f"{time}\n"
            text += f"Headline: {chapter['headline']}\n"
            text += f"Summary: {chapter['summary']}\n\n"

        return text
    else:
        text = "No chapters were detected."
        return text


def run_file(audio_url, file_name):
    global header
    data = main(audio_url)
    text_filename = file_name + ".txt"
    json_filename = file_name + ".json"
    text_file = open("./text/" + text_filename, 'x', encoding='utf8')
    json_file = open("./json/" + json_filename, 'x', encoding='utf8')

    text_file.write(data['text'])
    text_file.close()
    json_file.write(json.dumps(data, ensure_ascii=False, indent=2))
    json_file.close()

    sentiment = ""
    entities = ""
    keywords = ""
    topics = ""
    content_safety = ""
    chapters = ""

    if len(data['text']) != 0:
        sentiment = sentiment_analysis(data)
        entities = entity_detection(data)
        keywords = keyword_detection(data)
        topics = topic_detection(data)
        content_safety = content_safety_detection(data)
        chapters = chapter_detection(data)


    with open(f'enrichment.csv', 'a', newline='') as f:
        fieldnames = ['File Name','Topic Detection', 'Keyword Detection', 'Content Safety Detection', 'Entity Detection', 'Sentiment Analysis', 'Auto Chapters']
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
            'Sentiment Analysis': sentiment,
            'Auto Chapters': chapters
        })

    if (len(data.get('utterances'))) >= 1:
        diarization_filename = file_name + ".json"
        diarization_file = open("./diarization/" + diarization_filename, 'x')

        diarization_file.write(json.dumps(data['utterances'], indent=2))
        diarization_file.close()


f = open("urls.txt", "r")
# loop over each line in text file and append them to dictionary
for x in f:
    lines.append(x)

with ThreadPoolExecutor(10) as executor:
    for line in lines:
        # splits file names and urls
        split_line = line.split(",")
        split_url = split_line[1].split("\n")
        executor.submit(run_file, split_url[0], split_line[0])




