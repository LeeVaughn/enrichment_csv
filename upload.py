import os
import requests
from concurrent.futures import ThreadPoolExecutor


file_dir = "./audio"
uploaded_files = os.listdir(file_dir)


def upload_file(filename):
    print(f"{filename} uploading...")

    def read_file(filename, chunk_size=5242880):
        with open("./audio/" + filename, 'rb') as _file:
            while True:
                data = _file.read(chunk_size)
                if not data:
                    break
                yield data
    

    headers = {'authorization': "your api key here"}
    response = requests.post('https://api.assemblyai.com/v2/upload', headers=headers, data=read_file(filename))

    response = response.json()
    print(response["upload_url"])

    # write file names and URLs to a text file
    f = open("urls.txt", "a")
    f.write(filename + "," + response["upload_url"] + "\n")
    f.close()


with ThreadPoolExecutor(10) as executor:
    for x in range(0, len(uploaded_files)):
        executor.submit(upload_file, uploaded_files[x])
