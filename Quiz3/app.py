from flask import Flask,render_template, request, url_for, redirect, make_response      # import flask
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
from io import BytesIO
import pyodbc
import os, uuid
from azure.storage.blob import BlobServiceClient, BlobClient, ContainerClient, __version__, ContentSettings
import numpy as np
import base64
from matplotlib import pyplot as plt

connect_str = os.getenv('AZURE_STORAGE_CONNECTION_STRING')
connect_str ="DefaultEndpointsProtocol=https;AccountName=varshiniclass6331;AccountKey=DrPzSMpG0JYI/sXGEfpyc8ag7DjWkEmOlHOZW2w2g7h7KIPGj8QHQOXyD8Edmq8JAL8+GgyMIu726spVSHNo9w==;EndpointSuffix=core.windows.net"
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
    cursor.execute('DROP TABLE IF EXISTS election;')
    cursor.commit()
    cnxn.setdecoding(pyodbc.SQL_CHAR, encoding='utf-8')
    cnxn.setdecoding(pyodbc.SQL_WCHAR, encoding='utf-8')
    cnxn.setencoding(encoding='utf-8')
    sqlCreate = (
        'CREATE TABLE election ( '+
        'YEAR INT, '+
        'STATE TEXT, '+
        'STATE_PO TEXT, '+
        'CANDIDATE TEXT, '+
        'PARTY_DETAILED TEXT, '+
        'CANDIDATEVOTES INT, '+
        'TOTALVOTES INT, '+
        'PARTY_SIMPLIFIED TEXT '+
    ');').lower()

    #cursor.execute("CREATE TABLE People(name varchar(30), state varchar(25), salary int, grade varchar(5), room varchar(5), telnum varchar(15), picture varchar(20), keywords varchar(40)); ")
    cursor.execute(sqlCreate.lower())
    cursor.commit()
    csvfiles = os.listdir("/Users/srikanthadavalli/Cloud_computing/Quiz2/quakes/")
    print(csvfiles)
    with open('/Users/srikanthadavalli/Cloud_computing/Quiz2/quakes/presidentialelect.csv', newline='') as csvfile:
        data = list(csv.reader(csvfile))
    sqlInsert = "INSERT INTO election(year,state,state_po,candidate,party_detailed,candidatevotes,totalvotes,party_simplified) VALUES (?,?,?,?,?,?,?,?);"
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
account_name = "varshiniclass6331"
account_key = "DrPzSMpG0JYI/sXGEfpyc8ag7DjWkEmOlHOZW2w2g7h7KIPGj8QHQOXyD8Edmq8JAL8+GgyMIu726spVSHNo9w=="

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


@app.route('/pie',methods=['GET','POST'])
def pie():
    yeartemp = str(request.form['year1'])
    print(yeartemp)
    stateAbb = request.form['state1']
    print(stateAbb)
    sql ="SELECT totalvotes,party_detailed from election WHERE year = ? AND state_po LIKE ?"
    # group by CAST(locationSource as VARCHAR)"
    values = [yeartemp,str(stateAbb)]
    cursor.execute(sql,values)
    result_set = cursor.fetchall()
    print('result: ', result_set)
    age =[]
    name = []
    img1 = BytesIO()
    for row in result_set:
        age.append(row[0])
        name.append(row[1])

    print('age: ',age)
    labels = []
    for i in range(len(name)):
        labels.append(name[i])

    print('labels: ', labels)
    #labels = (name[0],name[1],name[2],name[3])
    #colors = ['gold', 'yellowgreen', 'blue']
    colors = ['gold', 'yellowgreen', 'lightcoral','gold', 'yellowgreen', 'lightcoral','gold', 'yellowgreen', 'lightcoral','red','green']
    sizes = []
    for i in range(len(age)):
        sizes.append(age[i])
    explode = []
    for i in range(len(sizes)):
        explode.append(0.1)
    #sizes = [age[0],age[1],age[2],age[3]]


    plt.pie(sizes, explode=explode, labels=labels, colors=colors,autopct='%1.1f%%', shadow=True, startangle=140)
    plt.axis('equal')
    plt.savefig(img1, format='png')
    img1.seek(0)
    plot_url = base64.b64encode(img1.getvalue())
    response1 = make_response(img1.getvalue())
    response1.headers['Content-Type'] = 'image/png'
    return response1

@app.route('/barchart',methods=['GET','POST'])
def barchart():
    minyear = request.form['minyear']
    maxyear = request.form['maxyear']
    stateAbb = request.form['state2']
    print(stateAbb)
    success="SELECT year, candidate from election WHERE year BETWEEN ? and ? AND state_po LIKE ?"
    values = [minyear,maxyear,str(stateAbb)]
    cursor.execute(success,values)
    result_set = cursor.fetchall()
    print('result: ', result_set)
    age =[]
    name = []
    img2 = BytesIO()
    for row in result_set:
       age.append(row[0])
       name.append(row[1])
    print('age: ', age)

    objects = []
    for i in range(len(name)):
        objects.append(name[i])
    #objects = ('0-2', '2-3', '3-5')
    y_pos = np.arange(len(objects))
   # performance = [0-1,1-2,2-3]
    print('pos: ',y_pos)
    performance = []
    for i in range(len(age)):
        performance.append(age[i])
    plt.bar(age, objects, align='center', alpha=0.4)
    plt.xticks(performance, objects)
    plt.ylabel('year')
    plt.title('candidates vs year')
    plt.savefig(img2, format='png')
    img2.seek(0)
    plot_url = base64.b64encode(img2.getvalue())
    response2 = make_response(img2.getvalue())
    response2.headers['Content-Type'] = 'image/png'
    return response2


@app.route('/scatter',methods=['GET','POST'])
def scatter():
    minyear = request.form['minyear']
    maxyear = request.form['maxyear']
    stateAbb = request.form['state2']
    print(stateAbb)
    success="SELECT year, candidate from election WHERE year BETWEEN ? and ? AND state_po LIKE ?"
    values = [minyear,maxyear,str(stateAbb)]
    cursor.execute(success,values)
    result_set = cursor.fetchall()
    print('result: ', result_set)
    age =[]
    name = []
    img3 = BytesIO()
    for row in result_set:
       age.append(row[0])
       name.append(row[1])
    print('age: ', age)

    objects = []
    for i in range(len(name)):
        objects.append(name[i])
    #objects = ('0-2', '2-3', '3-5')
    y_pos = np.arange(len(objects))
   # performance = [0-1,1-2,2-3]
    print('pos: ',y_pos)
    performance = []
    for i in range(len(age)):
        performance.append(age[i])
    plt.scatter(age, objects,  alpha=0.4)
    plt.xlabel('candidate')
    plt.ylabel('year')
    plt.title('candidates vs year')
    plt.savefig(img3, format='png')
    img3.seek(0)
    plot_url = base64.b64encode(img3.getvalue())
    response3 = make_response(img3.getvalue())
    response3.headers['Content-Type'] = 'image/png'
    return response3


# port = os.getenv('PORT', '8000')
if __name__ == "__main__":
    app.run(debug=True)
