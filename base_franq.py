import gspread
from oauth2client.service_account import ServiceAccountCredentials
import boto3
from botocore.exceptions import ClientError
import time
from unidecode import unidecode

# Informações de acesso ao banco de dados
aws_access_key_id = ''
aws_secret_access_key = ''
region_name = ''
database = ''
workgroup = ''

caminho_query = r'C:\Users\Carva\OneDrive\Área de Trabalho\Gustavo\Automações em Python\Automação_FRANQUIAS\base_franq.sql'

caminho_saida_csv = r'C:\Users\Carva\OneDrive\Área de Trabalho\Gustavo\Automações em Python\Automação_FRANQUIAS\resultado_query.csv'

def ler_query(caminho):
    with open(caminho, 'r') as file:
        query = file.read()
    return query

def run_query(query):
    try:
        response = athena_client.start_query_execution(
            QueryString=query,
            QueryExecutionContext={
                'Database': database
            },
            WorkGroup=workgroup
        )
        query_execution_id = response['QueryExecutionId']
        return query_execution_id
    except ClientError as e:
        print("Erro ao executar a query:", e)
        return None

scope = ['https://spreadsheets.google.com/feeds', 'https://www.googleapis.com/auth/drive']
creds = ServiceAccountCredentials.from_json_keyfile_name('C:/Users/Carva/OneDrive/Área de Trabalho/Gustavo/Automações em Python/Automação_FRANQUIAS/pythonintegrationapi-15c7447a756e.json', scope)
client = gspread.authorize(creds)

session = boto3.Session(
    aws_access_key_id=aws_access_key_id,
    aws_secret_access_key=aws_secret_access_key,
    region_name=region_name
)

athena_client = session.client('athena')

def remover_acentos(texto):
    return unidecode(texto)

planilha = client.open('SF Franquias | Meta - Banco')
aba = planilha.worksheet('db_realizado')

existing_data = aba.get_all_values()

query = ler_query(caminho_query)

query_execution_id = run_query(query)

if query_execution_id:
    print("Query executada com sucesso. ID da execução:", query_execution_id)
    print("Aguardando o resultado...")
    
    while True:
        response = athena_client.get_query_execution(
            QueryExecutionId=query_execution_id
        )
        status = response['QueryExecution']['Status']['State']
        if status in ['SUCCEEDED', 'FAILED', 'CANCELLED']:
            break
        time.sleep(5) 

    if status == 'SUCCEEDED':
        try:
            response = athena_client.get_query_results(
                QueryExecutionId=query_execution_id
            )
            with open(caminho_saida_csv, 'w', encoding='utf-8') as file:
                for idx, row in enumerate(response['ResultSet']['Rows']):
                    if idx == 0:  # Ignorar o cabeçalho
                        continue
                    clean_row = [remover_acentos(field.get('VarCharValue', '')) for field in row['Data']]  # Removendo acentos
                    file.write(';'.join(clean_row) + '\n')  # Alteração aqui para usar ponto e vírgula (;)
            print(f"Resultado da query exportado para: {caminho_saida_csv}")

            # Agora vamos importar os dados para a planilha
            new_data = []
            with open(caminho_saida_csv, 'r', encoding='utf-8') as file:
                for line in file:
                    new_data.append(line.strip().split(';'))

            new_rows = []
            for row in new_data:
                if row not in existing_data:
                    new_rows.append(row)

            if new_rows:
                next_row_index = len(existing_data) + 1
                for new_row in new_rows:
                    aba.append_row(new_row, value_input_option='USER_ENTERED', insert_data_option='INSERT_ROWS', table_range=f'A{next_row_index}')
                    next_row_index += 1
                    time.sleep(1)  
                print(f"Foram adicionadas {len(new_rows)} novas linhas à planilha.")
            else:
                print("Não há novos dados para adicionar.")

        except ClientError as e:
            print("Erro ao exportar o resultado da query:", e)
    else:
        print("A execução da query falhou.")
else:
    print("Falha ao executar a query.")
