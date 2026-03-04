#Importação de Bibliotecas
from processamentos_dados import Dados

#Caminho dos arquivos
path_json = "data_raw/dados_empresaA.json"
path_csv = "data_raw/dados_empresaB.csv"
path_dados_combinados = 'data_processed/dados_combinados.csv'

#Extract
dados_empresaA = Dados.leitura_dados(path_json, 'json')
print(f"Nome das Colunas Empresa A: {dados_empresaA.name_columns}")
print(f"Qtd de Linhas Empresa A: {dados_empresaA.qtd_linhas}")

dados_empresaB = Dados.leitura_dados(path_csv, 'csv')
print(f"Nome das Colunas Empresa B: {dados_empresaB.name_columns}")
print(f"Qtd de Linhas Empresa B: {dados_empresaB.qtd_linhas}")

#Transform
key_mapping = {'Nome do Item': 'Nome do Produto',
               'Classificação do Produto': 'Categoria do Produto',
               'Valor em Reais (R$)': 'Preço do Produto (R$)',
               'Quantidade em Estoque': 'Quantidade em Estoque',
               'Nome da Loja': 'Filial',
               'Data da Venda': 'Data da Venda'}

dados_empresaB.rename_columns(key_mapping)
print(f"Nome das Colunas da Empresa B depois do KeyMapping: {dados_empresaB.name_columns}")

dados_fusao = Dados.join(dados_empresaA, dados_empresaB)
print(f"Nome das Colunas depois do JOIN nas 2 tabelas das empresas: {dados_fusao.name_columns}")
print(f"Qtd  de Linhas da tabela combinada: {dados_fusao.qtd_linhas}")

#Load
dados_fusao.save_data(path_dados_combinados)
print(f"Os dados finais foram salvos nesse caminho: '{path_dados_combinados}'")