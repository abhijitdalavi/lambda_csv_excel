import boto3
import csv
import datetime
import pandas as pd
from io import BytesIO
from zipfile import ZipFile
from urllib.parse import unquote_plus


def lambda_handler(event, context):
    name = datetime.datetime.now().strftime('%y%m%d%H%M')
    s3_client = boto3.client('s3')
    s3_resource = boto3.resource('s3')
    tmpFolder = '/tmp/' 
    newFolder = 'descarga/' 
    bucket = event['Records'][0]['s3']['bucket']['name']
    key = unquote_plus(event['Records'][0]['s3']['object']['key'],encoding ='utf-8')
    
    
    try:
        response = s3_resource.Object(bucket_name=bucket, key=key)    
        buffer = BytesIO(response.get()["Body"].read())
        z = ZipFile(buffer)
        z.extractall(tmpFolder) 
    
        for filename in z.namelist():
            with open(tmpFolder +'newfile.csv',encoding='utf-8',mode='w',newline='') as file_out:
                csv_writer = csv.writer(file_out, delimiter=',', quotechar='"', quoting=csv.QUOTE_MINIMAL)
            
                line_count = 0
                with open(tmpFolder + filename,encoding='utf-8',mode='r') as csv_file:
                    csv_reader = csv.reader(csv_file, delimiter=',',quotechar='"')
                    for row in csv_reader:
                        if line_count == 0:
                            csv_writer.writerow([row[0],row[1], row[2], row[12],row[14],row[17],row[19],row[48],row[49]])
                        else:
                            if row[3] == row[4] == 'Yes':
                                if row[14].find('COLOMBIANA DE COMERCIO S.A Y/O ALKOSTO S.A') >=0 or row[14].find('MAKRO') >=0 or row[14].find('CENCOSUD') >=0 or row[14].find('COLSUBSIDIO') >=0 or row[14].find('RECETTA') >=0 or row[14].find('PANAMERICANA LIBRE') >=0 or row[14].find('Falabella') >=0 :
                                    csv_writer.writerow([row[0],row[1], row[2], row[12],row[14],row[17],row[19],row[48],row[49]])
                        line_count += 1
        
        df = pd.read_csv(tmpFolder +'newfile.csv')
        writer = pd.ExcelWriter(tmpFolder +'newfile.xlsx', engine='xlsxwriter')
        df.to_excel(writer,'Sheet1')
        writer.save()
        s3_client.upload_file(Filename= tmpFolder +'newfile.xlsx', Bucket= bucket,Key= newFolder + name + 'ListaItems.xlsx')
        
        z.close()
        print("Cargo el archivo")
    
    except Exception as e:
        print(e)
        raise(e)
