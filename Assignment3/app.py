from flask import Flask,render_template, request, url_for, redirect       # import flask
import os
from os import listdir
#from flask_session_azure import storage_account_interface
import csv,sys
import json
import pandas as pd
import sqlite3
import pyodbc
from datetime import datetime
from werkzeug.utils import secure_filename

import redis
import time
import random
import hashlib
import pickle

import pyodbc
import os, uuid
from azure.storage.blob import BlobServiceClient, BlobClient, ContainerClient, __version__, ContentSettings

connect_str = os.getenv('AZURE_STORAGE_CONNECTION_STRING')
connect_str = "DefaultEndpointsProtocol=https;AccountName=class6331;AccountKey=nVL4pbVCNSkhAqYKVrrEHyCyj33Rb8CMu83MTZZCQft/IijdVwf6t1VENwFZxKSNYyFx5SgyC6Z3QAD4q6/rAQ==;EndpointSuffix=core.windows.net"
print("connect_str")
print(connect_str)
#
gfile = ""
# # Create the BlobServiceClient object which will be used to create a container client
blob_service_client = BlobServiceClient.from_connection_string(connect_str)
print("bob_service_client")
#
# # Create a unique name for the container
container_name = 'myfiles'
print("container_name")
container_client = blob_service_client.from_connection_string(connect_str)


app = Flask(__name__)             # create an app instance

# # dir_path = os.path.dirname(os.path.realpath(__file__)) + '\\tmp'
dir_path = os.path.dirname(os.path.realpath(__file__))
print(dir_path)
UPLOAD_FOLDER = dir_path

ALLOWED_EXTENSIONS = set(['jpg', 'csv'])
CSV_ALLOWED_EXTENSIONS = set(['csv'])
app.config['UPLOAD_FOLDER'] = UPLOAD_FOLDER

r = redis.Redis(host='varshiniuta.redis.cache.windows.net',
        port=6380, db=0, password='3stf5BngVhOjxpakxWiFPJQpKVFZQI76wmeuihSKBGo=',ssl=True)

#driver = '/usr/local/lib/libmsodbcsql.13.dylib'
cnxn = pyodbc.connect('DRIVER='+driver+';PORT=1433;SERVER='+server+';PORT=1443;DATABASE='+database+';UID='+username+';PWD='+ password)
cursor = cnxn.cursor()
print('Connected...')

@app.route("/")                   # at the end point /
def home():                      # call method hello
    return render_template("index.html")         # which returns "hello world"

@app.route('/loadPeople', methods=['POST','GET'])
def loadPeople():
    cursor = cnxn.cursor()
    cursor.execute('DROP TABLE IF EXISTS earthquakes;')
    cursor.commit()
    cnxn.setdecoding(pyodbc.SQL_CHAR, encoding='utf-8')
    cnxn.setdecoding(pyodbc.SQL_WCHAR, encoding='utf-8')
    cnxn.setencoding(encoding='utf-8')
    sqlCreate = (
        'CREATE TABLE earthquakes ( '+
        'TIME DATETIME, '+
        'LATITUDE FLOAT, '+
        'LONGITUDE FLOAT, '+
        'DEPTH FLOAT, '+
        'MAG FLOAT, '+
        'MAGTYPE TEXT, '+
        'NST INT, '+
        'GAP FLOAT, '+
        'DMIN FLOAT, '+
        'RMS FLOAT, '+
        'NET TEXT, '+
        'ID TEXT, '+
        'UPDATED TEXT, '+
        'PLACE TEXT, '+
        'TYPE TEXT, '+
        'HORIZONTALERROR FLOAT, '+
        'DEPTHERROR FLOAT, '+
        'MAGERROR FLOAT, '+
        'MAGNST INT, '+
        'STATUS TEXT, '+
        'LOCATIONSOURCE TEXT, '+
        'MAGSOURCE TEXT '+
    ');').lower()

    #cursor.execute("CREATE TABLE People(name varchar(30), state varchar(25), salary int, grade varchar(5), room varchar(5), telnum varchar(15), picture varchar(20), keywords varchar(40)); ")
    cursor.execute(sqlCreate.lower())
    cursor.commit()
    csvfiles = os.listdir("/Users/srikanthadavalli/Cloud_computing/Assignment2/quakes/")
    print(csvfiles)
    with open('/Users/srikanthadavalli/Cloud_computing/Assignment2/quakes/all_week.csv', newline='') as csvfile:
        data = list(csv.reader(csvfile))

    sqlInsert = "INSERT INTO earthquakes(time,latitude,longitude,depth,mag,magType,nst,gap,dmin,rms,net,id,updated,place,type,horizontalError,depthError,magError,magNst,status,locationSource,magSource) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?);"
    del data[0]
    records = []
    for x in data:
        records.append(tuple(x))

    i = 0
    batch_size = 100
    record_count = len(records)
    while(True):
        s = i
        e = i + batch_size
        if(s < record_count):
            if(e > record_count):
                e = record_count
            cursor.executemany(sqlInsert, records[s:e])
            cnxn.commit()
            print(str(s) + " to " + str(e))
        else:
            break
        i = e

    return 'Created'

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
        print(allowed_file(file.filename))
        if file and allowed_file(file.filename):
            filename = secure_filename(file.filename)
            print(filename)

            content_settings = ContentSettings(content_type=file.content_type + "+xml")
            file.save(os.path.join(app.config['UPLOAD_FOLDER'], filename))
            uploadToServer(filename)
            img_url_with_sas_token = get_img_url_with_blob_sas_token(filename)
            gfile = file
            return render_template('display.html', img_url_with_sas_token = img_url_with_sas_token, filename = filename)
    return '<h1>Unsuccesfull: Error</h1>'

def uploadToServer(filePath):
    try:
        print("Azure Blob Storage v" + __version__ + " - Python quickstart sample")
        print("container_client")
        upload_file_path = os.path.join(dir_path+'/', filePath)

        # Create a blob client using the local file name as the name for the blob
        blob_client = blob_service_client.get_blob_client(container=container_name, blob=filePath)

        print("\nUploading to Azure Storage as blob:\n\t" + filePath)

        # Upload the created file
        with open(upload_file_path, "rb") as data:
            blob_client.upload_blob(data, overwrite=True)

        # Quick start code goes here

    except Exception as ex:
        print('Exception:')
        print(ex)

        container_client = blob_service_client.create_container(container_name)
        print("container_client")
        upload_file_path = os.path.join(dir_path+'/', filePath)

        # Create a blob client using the local file name as the name for the blob
        blob_client = blob_service_client.get_blob_client(container=container_name, blob=filePath)

        print("\nUploading to Azure Storage as blob:\n\t" + filePath)

        # Upload the created file
        with open(upload_file_path, "rb") as data:
            blob_client.upload_blob(data, overwrite=True, content_settings = content_settings)



def allowed_file(filename):
    return '.' in filename and \
           filename.rsplit('.', 1)[1].lower() in ALLOWED_EXTENSIONS

def allowed_csv_file(filename):
    return '.' in filename and \
           filename.rsplit('.', 1)[1].lower() in CSV_ALLOWED_EXTENSIONS

from azure.storage.blob import generate_container_sas, ContainerSasPermissions
account_name = "class6331"
account_key = "nVL4pbVCNSkhAqYKVrrEHyCyj33Rb8CMu83MTZZCQft/IijdVwf6t1VENwFZxKSNYyFx5SgyC6Z3QAD4q6/rAQ=="

def get_img_url_with_container_sas_token(blob_name):
    container_sas_token = generate_container_sas(
        account_name=account_name,
        container_name=container_name,
        account_key=account_key,
        permission=ContainerSasPermissions(read=True),
        expiry=datetime.utcnow() + timedelta(hours=1)
    )
    blob_url_with_container_sas_token = f"https://{account_name}.blob.core.windows.net/{container_name}/{blob_name}?{container_sas_token}"
    return blob_url_with_container_sas_token

from azure.storage.blob import generate_blob_sas, BlobSasPermissions
from datetime import datetime
from datetime import timedelta

# using generate_blob_sas
def get_img_url_with_blob_sas_token(blob_name):
    blob_sas_token = generate_blob_sas(
        account_name=account_name,
        container_name=container_name,
        blob_name=blob_name,
        account_key=account_key,
        permission=ContainerSasPermissions(read=True),
        expiry=datetime.utcnow() + timedelta(hours=1)
    )
    blob_url_with_blob_sas_token = f"https://{account_name}.blob.core.windows.net/{container_name}/{blob_name}?{blob_sas_token}"
    print(blob_url_with_blob_sas_token)
    return blob_url_with_blob_sas_token

@app.route('/displaydata', methods=['GET','POST'])
def displaydata():
    rangeFrom = str(request.form.get('rangefrom1'))
    print(rangeFrom)
    rangeTo = str(request.form.get('rangeto1'))
    print(rangeTo)
    num = str(request.form.get('nom1'))
    print('num: ', num)
    start = time.time()
    for i in range(0,int(num)):
        mag = round(random.uniform(float(rangeFrom),float(rangeTo)),1)
        sql = "SELECT * FROM earthquakes WHERE mag > ?"
        values = [str(mag)]
        cursor.execute(sql,values)
    end = time.time()
    executiontime = end - start
    return render_template('count.html', t=executiontime)

@app.route('/multiplerun', methods=['POST','GET'])
def multiplerun():
    rangeFrom = str(request.form.get('rangefrom'))
    print(rangeFrom)
    rangeTo = str(request.form.get('rangeto'))
    print(rangeTo)
    num = str(request.form.get('nom'))
    start = time.time()
    for i in range(0,int(num)):
        mag = round(random.uniform(float(rangeFrom),float(rangeTo)),1)
        sql = "SELECT * FROM earthquakes WHERE mag > ?"
        values = [str(mag)]
        hash = hashlib.sha224(sql.encode('utf-8')).hexdigest()
        key = "redis_cache:" + hash
        if r.get(key):
            print("redis cached")
        else:
            cursor.execute(sql,values)
            data = cursor.fetchall()
            rows = []
            for j in data:
                rows.append(str(j))
            r.set(key,pickle.dumps(list(rows)))
            r.expire(key,36)
        cursor.execute(sql,values)
    end = time.time()
    executiontime = end - start
    return render_template('count.html', t=executiontime)


# port = os.getenv('PORT', '8080')
if __name__ == "__main__":
    app.run(debug=True)
