from flask import Flask,render_template, request, url_for, redirect       # import flask
import os
from os import listdir
#from flask_session_azure import storage_account_interface
import csv,sys
import json
import pandas as pd
import sqlite3
import pyodbc
from werkzeug.utils import secure_filename

import pyodbc
import os, uuid
from azure.storage.blob import BlobServiceClient, BlobClient, ContainerClient, __version__, ContentSettings

connect_str = os.getenv('AZURE_STORAGE_CONNECTION_STRING')
connect_str = "DefaultEndpointsProtocol=https;AccountName=class6331;AccountKey=nVL4pbVCNSkhAqYKVrrEHyCyj33Rb8CMu83MTZZCQft/IijdVwf6t1VENwFZxKSNYyFx5SgyC6Z3QAD4q6/rAQ==;EndpointSuffix=core.windows.net"
print("connect_str")
print(connect_str)

gfile = ""
# Create the BlobServiceClient object which will be used to create a container client
blob_service_client = BlobServiceClient.from_connection_string(connect_str)
# sas_Token = generate_blob_sas(
#     account_name=account_name,
#     container_name=container_name,
#     blob_name=blob_name,
#     account_key=account_key,
#     permission=ContainerSasPermissions(read=True),
#     expiry=datetime.utcnow() + timedelta(hours=1)
# )
# blob_service_client = BlobServiceClient(account_url="https://{account_name}.blob.core.windows.net", credential=sas_token)
print("bob_service_client")

# Create a unique name for the container
container_name = 'myfiles'
print("container_name")
container_client = blob_service_client.from_connection_string(connect_str)

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

 # overwrite=True,

app = Flask(__name__)             # create an app instance

# dir_path = os.path.dirname(os.path.realpath(__file__)) + '\\tmp'
dir_path = os.path.dirname(os.path.realpath(__file__))
print(dir_path)
UPLOAD_FOLDER = dir_path

ALLOWED_EXTENSIONS = set(['jpg', 'csv'])
CSV_ALLOWED_EXTENSIONS = set(['csv'])
app.config['UPLOAD_FOLDER'] = UPLOAD_FOLDER


server = 'myazureserver6331.database.windows.net'
database = 'varshiniCSE6331'
username = 'azureuser'
password = 'Varshini_4'
driver = '{ODBC Driver 13 for SQL Server}'
#driver = '/usr/local/lib/libmsodbcsql.13.dylib'
cnxn = pyodbc.connect('driver={ODBC Driver 17 for SQL Server};Server=tcp:myazureserver6331.database.windows.net,1433;Database=varshiniCSE6331;Uid=azureuser;Pwd=Varshini_4;Encrypt=yes;TrustServerCertificate=no;Connection Timeout=30;')
cursor = cnxn.cursor()
print('Connected...')

@app.route("/")                   # at the end point /
def home():                      # call method hello
    return render_template("index.html")         # which returns "hello world"

@app.route('/loadPeople', methods=['POST','GET'])
def loadPeople():
    cursor = cnxn.cursor()
    cursor.execute('DROP TABLE IF EXISTS Data;')
    cursor.commit()
    cnxn.setdecoding(pyodbc.SQL_CHAR, encoding='utf-8')
    cnxn.setdecoding(pyodbc.SQL_WCHAR, encoding='utf-8')
    cnxn.setencoding(encoding='utf-8')
    # print(gfile)
    # if gfile and allowed_csv_file(file.filename):
    #     filename = secure_filename(file.filename)
    #     print(filename)
    #     source = get_img_url_with_blob_sas_token(filename)
    #     df = pd.read_csv(source)
    #     print("df")
    #     print(df)
    sqlCreate = "CREATE TABLE Data(name varchar(30), nsize int, distance int, author varchar(20), picture varchar(20), keywords varchar(40)); "
    #cursor.execute("CREATE TABLE People(name varchar(30), state varchar(25), salary int, grade varchar(5), room varchar(5), telnum varchar(15), picture varchar(20), keywords varchar(40)); ")
    cursor.execute(sqlCreate)
    cursor.commit()
    csvfiles = os.listdir("/Users/srikanthadavalli/Cloud_computing/Quiz1/Files/")
    print(csvfiles)
    for file in csvfiles:
        filename = os.path.splitext(file)[0]
        filepath = "/Users/srikanthadavalli/Cloud_computing/Quiz1/Files/"+filename+".csv"
        print(filepath)
        col_names = ['name','nsize','distance','author','picture','keywords']
        csvData = pd.read_csv(filepath, names=col_names, header=None, skiprows = [0])
        df = csvData.fillna(0)


        for i, line in df.iterrows():
            sqlInsert = 'INSERT INTO Data (name,nsize,distance,author,picture,keywords) VALUES (?,?,?,?,?,?)'
            distance = 0
            nsize = 0

            try:
                distance = int(line['distance'])
                nsize = int(line['nsize'])
            except:
                distance = 0
                nsize = 0
            print("dist")
            print(line['distance'])

            value = (str(line['name']),nsize, distance,str(line['author']),str(line['picture']),str(line['keywords']))
            #print(value)
            print(line.distance)
            #cursor.execute( 'INSERT INTO People (name,state,salary,grade,room,telnum,picture,keywords) VALUES (?,?,?,?,?,?,?,?)', line.name,line.state,line.salary,line.grade,line.room,line.telnum,line.picture,line.keywords)

            cursor.execute(sqlInsert,value)
        cursor.execute('SELECT count(*) from Data')
        print(cursor.fetchall())
        cursor.commit()

        print(i,line['name'])
    return "created"

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

def allowed_file(filename):
    return '.' in filename and \
           filename.rsplit('.', 1)[1].lower() in ALLOWED_EXTENSIONS

def allowed_csv_file(filename):
    return '.' in filename and \
           filename.rsplit('.', 1)[1].lower() in CSV_ALLOWED_EXTENSIONS


# @app.route('/searchPictures', methods=['POST','GET'])
# def searchPictures():
#     cursor.execute("SELECT picture FROM people WHERE salary < 99000")
#     cursorData = cursor.fetchone()
#     rows = []
#
#     while cursorData is not None:
#         if cursorData[0].strip():
#             print(cursorData[0])
#             rows.append(cursorData[0])
#         cursorData = cursor.fetchone()
#
#     print("added rows")
#     print(rows)
#     return sasEncodePictures(rows)

@app.route('/searchNsize', methods=['POST','GET'])
def searchNsize():
    min = request.form.get('minOne')
    max = request.form.get('maxOne')
    authorName = str(request.form.get('authorName'))
    print(authorName)
    print(min,max)

    query = "SELECT * FROM data WHERE author = ? AND nsize BETWEEN ? AND ?"
    #query = "SELECT * FROM data WHERE author = ?"
    value = [authorName,min,max]
   #value = [authorName]
    cursor.execute(query,value)
    cursorData = cursor.fetchall()
    print(cursorData)
    rows = []

    # while cursorData is not None:
    #     if cursorData[0].strip():
    #         print(cursorData[0])
    #         rows.append(cursorData[0])
    #     cursorData = cursor.fetchone()

    print("added rows")
    print(rows)
    return render_template('list.html',rows=cursorData)


@app.route('/update',methods=['POST','GET'])
def update():
    column = str(request.form['column'])
    print(column)
    name = request.form['nam']
    print("name" + name)
    desc = str(request.form['desc'])
    print(desc)
    query = "UPDATE data SET picture = ? WHERE name = ?"
    sqlQuery = 'SELECT * FROM data where name = ?'
    if column == 'keywords':
        query = "UPDATE data SET keywords = ? WHERE name = ?"
    elif column == 'author':
        query = "UPDATE data SET author = ? WHERE name = ?"
    elif column == 'picutre':
        query = "UPDATE data SET picture = ? WHERE name = ?"
    elif column == 'distance':
        query = "UPDATE data SET distance = ? WHERE name = ?"
    elif column == 'nsize':
        query = "UPDATE data SET nsize = ? WHERE name = ?"
    values = [ desc, name]
    cursor.execute(query,values)
    print('Updated Sucessfully')
    print(cursor)
    cursor.commit()
    queryValue= [name]
    print(queryValue)
    cursor.execute(sqlQuery,queryValue)
    rows = cursor.fetchone()
    print(rows)
    return render_template('updateDisplay.html', rows=rows)
    #return viewTable(cursor)

@app.route('/viewTable', methods=['GET'])
def viewTable(cursor):
    cursor.execute('SELECT * FROM people')
    rows=cursor.fetchall()
    print(rows)
    cursor.commit()
    #return render_template('list.html', rows=rows)
    return sasEncodePictures(rows)


@app.route('/deleterecord', methods=['GET', 'POST'])
def deleterecord():
    name1 = str(request.form['dname'])
    print("name" + name1)
    print("step1")
    querry = "DELETE FROM people WHERE name = ?"
    print("step2")
    val = [name1]
    cursor.execute(querry,val)
    print("step3")
    cursor.commit()
    cursor.execute('SELECT * FROM people')
    rows=cursor.fetchall()
    print(rows)

    return render_template('list.html',rows=rows)

@app.route('/selectQueryByRange', methods=['POST','GET'])
def selectQueryByRange():
    min1 = request.form['min1']
    max1 = request.form['max1']
    #print(firstname)
    query = "select * from data WHERE distance BETWEEN ? AND ? "
    values = [min1,max1]
    cursor.execute(query,values)
    # cursor.execute("SELECT picture FROM people WHERE name ='"+ firstname+"'")
    rows = cursor.fetchall()
    print(rows)
    # return sasEncodePictures(rows)
    return render_template('list.html', rows=rows)

def sasEncodePictures(rows):
    row_sas = {}
    for row in rows:
        print(row)
        print("row")
        print(row)
        img_url_with_sas_token = get_img_url_with_blob_sas_token(row)
        row_sas[row] = img_url_with_sas_token
    print(row_sas)
    return render_template('list.html', rows = row_sas)


from azure.storage.blob import generate_container_sas, ContainerSasPermissions
account_name = "class6331"
account_key = "nVL4pbVCNSkhAqYKVrrEHyCyj33Rb8CMu83MTZZCQft/IijdVwf6t1VENwFZxKSNYyFx5SgyC6Z3QAD4q6/rAQ=="
# container_name = "<your container name, such as `test`>"

# using generate_container_sas
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



# port = os.getenv('PORT', '8080')
if __name__ == "__main__":
    app.run(debug=True)