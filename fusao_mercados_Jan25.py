import pandas as pd

class DataProcessor:
    def __init__(self, path_json, path_csv):
        # Inicializa os caminhos dos arquivos
        self.path_json = path_json
        self.path_csv = path_csv
        self.empresaA = None
        self.empresaB = None
        self.dados_consolidados1 = None
    
    # Extração: Leitura dos arquivos JSON e CSV
    def read_files(self):
        self.empresaA = pd.read_json(self.path_json)
        self.empresaB = pd.read_csv(self.path_csv)
    
    # Transformação: Unificação dos cabeçalhos e adição de nova coluna
    def unify_headers(self):
        # Adicionar a coluna 'Data da Venda' com valor 'Indisponível' se não existir
        if 'Data da Venda' not in self.empresaA.columns:
            self.empresaA['Data da Venda'] = 'Indisponível'
        
        # Verificar se os cabeçalhos são iguais e renomear se necessário
        if list(self.empresaA.columns) != list(self.empresaB.columns):
            self.empresaB.columns = self.empresaA.columns
    
    # Transformação: Concatenar os DataFrames
    def concatenate_dataframes(self):
        # Concatenar os DataFrames verticalmente (um embaixo do outro)
        self.dados_consolidados1 = pd.concat([self.empresaA, self.empresaB], ignore_index=True)
    
    # Carga: Salvar o DataFrame consolidado em um novo arquivo CSV
    def save_to_csv(self, output_path, sep=','):
        self.dados_consolidados1.to_csv(output_path, sep=sep, index=False, encoding='utf-8')
    
    # Função principal que coordena todo o processo ETL
    def process_data(self, output_path, sep=','):
        self.read_files()            # Extração
        self.unify_headers()         # Transformação
        self.concatenate_dataframes() # Transformação

        # Verificação: Verificar a quantidade de linhas 
        print('Qtde linhas esperada: ', len(self.empresaA) + len(self.empresaB)) 
        print('Qtde linhas Consolidado:', len(self.dados_consolidados1))

        self.save_to_csv(output_path, sep=sep) # Carga

# Caminhos dos arquivos
path_json = 'data_raw/dados_empresaA.json'
path_csv = 'data_raw/dados_empresaB.csv'
output_path = 'data_processed/dados_consolidados1.csv'

# Criar uma instância da classe e processar os dados
processor = DataProcessor(path_json, path_csv)
processor.process_data(output_path, sep=',')

# Exibir o DataFrame combinado
processor.dados_consolidados1