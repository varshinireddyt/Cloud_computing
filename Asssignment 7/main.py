# Copyright 2018 Google LLC


from flask import Flask,render_template, request, url_for, redirect
import glob
import pandas as pd
import os
import string
# import nltk
import OpenSSL
#nltk.download()
# from nltk.corpus import stopwords
#
# from nltk.stem.porter import PorterStemmer
# from nltk.stem import WordNetLemmatizer
from bs4 import BeautifulSoup

from google.cloud import storage
# from spacy.lang.en import English
# from spacy.lang.en.stop_words import STOP_WORDS

app = Flask(__name__)

@app.route('/')
def hello():
    """Return a friendly HTTP greeting."""
    return render_template('index.html')
def remove_html(text):
    return BeautifulSoup(text, "lxml").text

os.environ["GOOGLE_APPLICATION_CREDENTIALS"]="cse6331-309022-d923e681d488.json"
file_location = "cse6331-309022.appspot.com/books/"
#app.config['file_location'] = file_location
filename = []
df = pd.DataFrame(columns=['book','data'])
client = storage.Client()
bucket = client.get_bucket('cse6331-309022.appspot.com')
print(bucket)

blobs = list(bucket.list_blobs(prefix = "books/"))
for blob in blobs:
    textline = blob.download_as_string()
    path = blob.name
    head, tail = os.path.split(path)
    print('tail: ',tail)
    if tail:
        textline = remove_html(textline)
        textline = textline.lower()
            #print(textline)
        df = df.append({'book': tail, 'data': textline},ignore_index=True)
print(df.head(9))

@app.route('/upload', methods=['GET', 'POST'])
def upload():
    print(request)
    print(request.method)
    print(request.files)
    if request.method == 'GET':
        return '<h1>Unsuccesfull: GET Error</h1>'
    if request.method == 'POST':
        # check if the post request has the file part
        if 'file' not in request.files:
            print('No file part')
            '<h1>Unsuccesfull: File Not found</h1>'
        file = request.files['file']
        # if user does not select file, browser also
        # submit a empty part without filename
        if file.filename == '':
            print('No selected file')
            '<h1>Unsuccesfull: No file selected </h1>'
        print(file)
        print(file.filename)
        blob = bucket.blob(file.filename)
        blob.upload_from_filename(file.filename)
        print('bucket: ', bucket)
        filename = file.filename
        return render_template('display.html', filename = filename)
    return '<h1>Unsuccesfull: Error</h1>'

@app.route('/allcount', methods=['POST','GET'])
def allcount():
    count = {}
    for idx, row in df.iterrows():
        print(1)
        temp = row['data']
        count[row['book']] = len(temp)
    return render_template('allcount.html', count =count)


#os.path.join("books",'*.txt')
# for f in glob.iglob(os.path.join(file_location,'*.txt')):
#     filename.append(f)
#     print('f',f)
#     with open(f, 'r') as file:
#         textline = file.read().lower()
#         textline = remove_html(textline)
#         #print(textline)
#         df = df.append({'book': f, 'data': textline},ignore_index=True)

PUNCT_TO_REMOVE = string.punctuation
def remove_punctuation(text):
    """custom function to remove the punctuation"""
    return text.translate(str.maketrans('', '', PUNCT_TO_REMOVE))
df['data'] = df['data'].apply(lambda text: remove_punctuation(text))

stopWords = []

cwd = os.getcwd()
with open(os.path.join(cwd,'shortliststopwords.txt')) as f:
    for word in f:
        word = word.split('\n')
        stopWords.append(word[0])

def removeStopwords(text):
    text = text.split()
    filteredText = [word for word in text if word not in stopWords]
    return filteredText

df['data'] = df['data'].apply(lambda text: removeStopwords(text))
print(df.head())


#11
@app.route('/viewstopwords', methods=['POST','GET'])
def viewstopwords():
    filteredText = {}
    for idx, row in df.iterrows():
        temp = row['data']
        filteredText[row['book']] = temp[:50]
    print('stop1: ', filteredText)
    return render_template('stopwordDisplay.html',filteredText = filteredText)


#12
@app.route('/wordcount', methods=['POST','GET'])
def wordcount():
    word1 = str(request.form.get('word1')).lower()
    f = str(request.form.get('file1'))
    f = f + '.txt'
    print('f: ', f)
    count = {}
    indexes = []
    line = []
    for idx, row in df.iterrows():

        if f == row['book']:
            docCount = 0

            temp = row['data']
        #temp = temp.split(" ")
        #print(temp)
            index = None
            try:
                index = temp.index(word1)
            except ValueError:
                continue
            for i in range(index, len(temp)):
                if temp[i] == word1:
                    docCount += 1
            count[f] = docCount
            indexes.append(index)

            for i in range(index, len(temp)):
                if temp[i] == word1:
                    indexes.append(i)

            for i in indexes:
                line.append(' '.join(temp[i:i + 20]))

    return render_template('count.html', count = count, line = line)






#print(stopWords)
# root = os.path.dirname(os.path.abspath(__file__))
# download_dir = os.path.join(root, 'nltk_data')
# os.chdir(download_dir)
# print(""download_dir)
# nltk.data.path.append(download_dir)


# STOPWORDS = set(stopwords.words('english'))
# def remove_stopwords(text):
#     """custom function to remove the stopwords"""
#     return " ".join([word for word in str(text).split() if word not in STOPWORDS])
#
# df['data'] = df['data'].apply(lambda text: remove_stopwords(text))

#print(df.head())

# stemmer = PorterStemmer()
# def stem_words(text):
#     return " ".join([stemmer.stem(word) for word in text.split()])
#
# df["data"] = df["data"].apply(lambda text: stem_words(text))
#
# lemmatizer = WordNetLemmatizer()
# def lemmatize_words(text):
#     return " ".join([lemmatizer.lemmatize(word) for word in text.split()])
#
# df["data"] = df["data"].apply(lambda text: lemmatize_words(text))
# print(df['data'])
#
# findWord = 'gutenberg'
# for idx,row in df.iterrows():
#     temp = row['data']
#     temp = temp.split(" ")
#     print(temp)
#     indexes = []
#     index = temp.index(findWord)
#     print('length : ',len(temp))
#     indexes.append(index)
#     for index in range(index + 1, len(temp)):
#         if temp[index] == findWord:
#             indexes.append(index)
#     for idx in indexes:
#         # line.append(' '.join(temp[idx:idx + 20]))
#         print(' '.join(temp[idx:idx + 20]))

    # pos = 0
    # for i,word in enumerate(temp):
    #     pos += (1+len(word))
    #     if i>=index:
    #         break
    # print('position: ',pos)
    # pos = index
    # print('found line: ', temp[pos])
    # print(' '.join(temp[pos:pos+20]))
    # print('filename: ', row['book'])
@app.route('/searchlines', methods=['POST','GET'])
def searchlines():
    findWord = str(request.form.get('word2')).lower()
    num = int(request.form.get('num'))
    print('findWord: ', findWord)
    findWord = findWord.split(' ')
    print('input word: ', findWord)
    line = []
    for idx, row in df.iterrows():
        print(1)
        temp = row['data']
        #temp = temp.split(" ")
        #print(temp)
        indexes = []

        index = None
        try:
            index = temp.index(findWord[0])
        except ValueError:
            continue

        indexes.append(index)

        for i in range(index, len(temp)):
            if len(findWord) > 1 and (findWord[0] == temp[i] and findWord[1] == temp[i+1]):
                indexes.append(i)

            elif temp[i] == findWord[0] and len(findWord) == 1:
                indexes.append(i)

        for i in indexes:
            line.append(' '.join(temp[i:i + num+10]))
        filename.append(row['book'])
        print('line: ',line)
    return render_template('result.html',line = line, filename=filename )

@app.route('/search', methods=['POST','GET'])
def search():
    findWord = str(request.form.get('find')).lower()
    print('findWord: ', findWord)
    findWord = findWord.split(' ')
    print('input word: ', findWord)
    line = []
    for idx, row in df.iterrows():
        print(1)
        temp = row['data']
        #temp = temp.split(" ")
        #print(temp)
        indexes = []

        index = None
        try:
            index = temp.index(findWord[0])
        except ValueError:
            continue

        indexes.append(index)

        for i in range(index, len(temp)):
            if len(findWord) > 1 and (findWord[0] == temp[i] and findWord[1] == temp[i+1]):
                indexes.append(i)

            elif temp[i] == findWord[0] and len(findWord) == 1:
                indexes.append(i)

        for i in indexes:
            line.append(' '.join(temp[i:i + 20]))
        filename.append(row['book'])
        print('line: ',line)
    return render_template('result.html',line = line, filename=filename )



if __name__ == '__main__':
    # This is used when running locally only. When deploying to Google App
    # Engine, a webserver process such as Gunicorn will serve the app. This
    # can be configured by adding an `entrypoint` to app.yaml.
    app.run( debug=True)
# [END gae_python3_app]
# [END gae_python38_app]
