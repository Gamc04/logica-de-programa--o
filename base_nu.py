import boto3
import time
import csv
from google.oauth2 import service_account
import gspread
from oauth2client.service_account import ServiceAccountCredentials
import os
import pandas as pd
import re
import unidecode


aws_access_key_id = 'AKIA4LTBLLTUAGB3EIP7'
aws_secret_access_key = '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'
region_name = 'us-east-1'
database = '7-smartfit-da-de-lake-artifacts-athena-mkt'
workgroup = 'mkt'  


client = boto3.client('athena', region_name=region_name,
                      aws_access_key_id=aws_access_key_id,
                      aws_secret_access_key=aws_secret_access_key)


def run_query(query, database, workgroup):
    response = client.start_query_execution(
        QueryString=query,
        QueryExecutionContext={'Database': database},
        ResultConfiguration={'OutputLocation': 's3://7-smartfit-da-de-lake-artifacts-athena-mkt'}, 
        WorkGroup=workgroup  
    )
    return response['QueryExecutionId']

def wait_for_query_to_complete(query_id):
    while True:
        response = client.get_query_execution(QueryExecutionId=query_id)
        state = response['QueryExecution']['Status']['State']

        if state == 'SUCCEEDED':
            print("Consulta concluída com sucesso.")
            return True
        elif state in ['FAILED', 'CANCELLED']:
            print(f"Consulta {state.lower()}.")
            return False
        else:
            print("Ainda executando...")
            time.sleep(5)

def remove_accents(data_row):
    return [unidecode.unidecode(field) if isinstance(field, str) else field for field in data_row]

def remove_duplicates_from_sheet(sheet):
    data = sheet.get_all_values()
    new_data = [data[0]]  

    for row in data[1:]:
        if row not in new_data:
            new_data.append(row)

    sheet.clear()
    sheet.update(new_data)

def find_first_empty_row(sheet):
    col_values = sheet.col_values(1)  
    return col_values.index('') + 1 if '' in col_values else sheet.row_count + 1  

with open(r'C:\Users\Carva\OneDrive\Área de Trabalho\Gustavo\Automação_python\base_nu.sql', 'r') as file:
    query = file.read()


query_id = run_query(query, database, workgroup)


if not wait_for_query_to_complete(query_id):
    print("Falha ou cancelamento na execução da consulta.")
    exit()


def get_results(query_id):
    results = []
    response = client.get_query_results(QueryExecutionId=query_id)
    results.extend(response['ResultSet']['Rows'])

    while 'NextToken' in response:
        response = client.get_query_results(QueryExecutionId=query_id, NextToken=response['NextToken'])
        results.extend(response['ResultSet']['Rows'])

    return results


result_rows = get_results(query_id)

csv_file_path = 'base_nu.csv'

with open(csv_file_path, 'w', newline='', encoding='UTF-8') as file:
    writer = csv.writer(file)
    for row in result_rows:
        cleaned_row = remove_accents([field.get('VarCharValue', '') for field in row['Data']])
        writer.writerow(cleaned_row)

KEY_FILE_PATH = r'C:\Users\Carva\OneDrive\Área de Trabalho\Gustavo\Automação_python\pythonintegrationapi-15c7447a756e.json'
sheet_name = 'Smart Fit Novas Unidades | PRÓPRIAS - TESTES'
file_path = csv_file_path
target_sheet_name = 'db_vendas'


def import_to_sheets(KEY_FILE_PATH, sheet_name, target_sheet_name, file_path):
    scope = ["https://spreadsheets.google.com/feeds", 'https://www.googleapis.com/auth/spreadsheets',
            "https://www.googleapis.com/auth/drive.file", "https://www.googleapis.com/auth/drive"]
    credentials = ServiceAccountCredentials.from_json_keyfile_name(KEY_FILE_PATH)
    client = gspread.authorize(credentials)
    print("Credenciais validadas")
    spreadsheet = client.open(sheet_name)
    target_sheet = None
    try:
        target_sheet = spreadsheet.worksheet(target_sheet_name)  
    except gspread.exceptions.WorksheetNotFound:
        print(f"A aba '{target_sheet_name}' não foi encontrada na planilha '{sheet_name}'.")
        return
    
    first_empty_row = find_first_empty_row(target_sheet)

    with open(file_path, 'r', encoding='utf-8') as file_obj:
        reader = csv.reader(file_obj)
        rows_to_append = list(reader)
        
        if rows_to_append:
            rows_to_append = rows_to_append[1:]
            
        if rows_to_append:  
            target_sheet.append_rows(rows_to_append, table_range=f'A{first_empty_row}')  

    remove_duplicates_from_sheet(target_sheet)

import_to_sheets(KEY_FILE_PATH, sheet_name, target_sheet_name, file_path)
