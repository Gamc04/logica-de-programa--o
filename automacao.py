import gspread
from oauth2client.service_account import ServiceAccountCredentials

scope = ['https://spreadsheets.google.com/feeds', 'https://www.googleapis.com/auth/drive']
creds = ServiceAccountCredentials.from_json_keyfile_name(r'C:\Users\Carva\OneDrive\Área de Trabalho\Gustavo\Automações em Python\Automação_NU\pythonintegrationapi-15c7447a756e.json', scope)
client = gspread.authorize(creds)

spreadsheet = client.open('Diário | Smart Fit Novas Unidades | PRÓPRIAS')
worksheet = spreadsheet.worksheet("informações")

valores_celula = worksheet.range('D2:D150')

valores_formatados = [f"'{celula.value}'" for celula in valores_celula if celula.value]

valores_para_sql = ', '.join(valores_formatados) if valores_formatados else ''

caminho_arquivo = r'C:\Users\Carva\OneDrive\Área de Trabalho\Gustavo\Automações em Python\Automação_NU\base_nu.sql'

try:
    with open(caminho_arquivo, 'r') as arquivo:
        linhas = arquivo.readlines()
        linhas[77] = f"{valores_para_sql}\n"

    with open(caminho_arquivo, 'w') as arquivo:
        arquivo.writelines(linhas)

    print("Arquivo base_nu.sql atualizado com sucesso.")
except Exception as e:
    print(f"Ocorreu um erro ao tentar atualizar o arquivo: {e}")

print("Valores obtidos do Google Sheets:", valores_para_sql)
print("Caminho do arquivo SQL:", caminho_arquivo)
