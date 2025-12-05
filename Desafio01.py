from bs4 import BeautifulSoup
import logging
import pandas as pd
import os
from pathlib import Path

NOME_LOG = "etl_pipeline.log"
NOME_CSV = "ofertas_calculadas.csv"
HTML_DATA = "index.html"
# --- Configuração de Caminho ---
# Garante que o diretório de execução do script seja usado
diretorio_do_script = Path(__file__).resolve().parent

#Definir o caminho completo da nova pasta
output_path = diretorio_do_script / "output"

# Criar a pasta se ela não existir
# parents=True: Cria quaisquer diretórios pai que não existam (se output_path fosse 'pasta_a/output')
# exist_ok=True: Não levanta um erro se o diretório já existir (o que você quer)
output_path.mkdir(parents=True, exist_ok=True)

# Caminhos completos para os arquivos
log_file_path = output_path / NOME_LOG
csv_file_path = output_path / NOME_CSV


# --- Configuração do Logging ---
logging.basicConfig(level=logging.INFO,
                    format='%(asctime)s - %(levelname)s - %(message)s',
                    handlers=[
                        logging.FileHandler(log_file_path),
                        logging.StreamHandler()
                    ])
logger = logging.getLogger(__name__)


# --- 1. Etapa de Extração ---
def extracao_dados(url: str) -> list:

    lista_produtos=[]
    soup = BeautifulSoup(url, 'html.parser')

    logger.info(f"Iniciando Extração dos Produtos.")

    produtos = soup.find_all('div', class_='product-card')

    for produto in produtos:

        try:
            id = produto.get('id')
            nome = produto.find('h2').text.strip()
            preco = produto.find('p', class_='price').text.strip()
            avaliacao = produto.find('span', class_='rating').text.strip()
            url_imagem = produto.find('img')['src']
            data = produto.get('data-date')
            tag_desconto = produto.find('span', class_='discount-rate')
            if(tag_desconto==None):
                logger.warning(f"Produto '{id}' - '{nome}' ignorado por falta da tag desconto.")
                continue
            taxa_desconto = tag_desconto.text.strip()
       
            # Armazenar os dados em um dicionário
            produto = {
                "nome": nome,
                "preco": preco,
                "avaliacao": avaliacao,
                "url_imagem": url_imagem,
                "data_cadastro": data,
                "taxa_desconto": taxa_desconto  
            }
            lista_produtos.append(produto)
        except Exception as e:
            logger.error(f"Erro durante a extração dos produtos: {e}")
            break
    return lista_produtos


# --- 2. Etapa de Transformação ---
def transformacao_dados(data: list) -> pd.DataFrame:

    logger.info("Iniciando Transformação de dados.")

    if not data:
        logger.warning("Lista de dados vazia. Pulando a transformação.")
        return pd.DataFrame()
    
    df = pd.DataFrame(data)

    logger.info("Conversão Numérica")
    # Conversão Numérica
  
    df['preco'] = df['preco'].str.replace('R$', '', regex=False).str.replace('.', '', regex=False).str.replace(',', '.', regex=False)
    df['preco_numerico'] = pd.to_numeric(df['preco'], errors='coerce').round(2)
    #
    logger.info("Transformando desconto (Ex: 10% OFF) em float (desconto_percentual).")
    df['taxa_desconto'] = df['taxa_desconto'].str.replace('% OFF', '', regex=False).str.replace('%', '', regex=False)
    df['desconto_percentual'] = pd.to_numeric(df['taxa_desconto'], errors='coerce').fillna(0.0)
    #
    logger.info("Normalizando a avaliação para uma escala de 0 a 100 (avaliacao_percentual)")
    df['avaliacao'] = df['avaliacao'].str.replace('/5.0', '', regex=False)
    df['avaliacao_percentual'] = pd.to_numeric(df['avaliacao'], errors='coerce') * 20

    #Formatação de Data
    logger.info("Formatação de Data")
    df['data'] = df['data_cadastro'].str.replace('/', '-', regex=False)
   
    #Cálculo do preço líquido
    logger.info("Cálculo do preço líquido")
    df['preco_liquido'] = df['preco_numerico'] * (1 - df['desconto_percentual'] / 100)
    df['preco_liquido'] = df['preco_liquido'].round(2)

    # Selecionar e reordenar colunas relevantes
    final_columns = [
        'nome', 'preco_numerico', 'desconto_percentual', 'preco_liquido', 'avaliacao_percentual','data'
    ]
    # Garante que apenas as colunas existentes sejam selecionadas
    df_transformado = df[[col for col in final_columns if col in df.columns]]
    
    logger.info("Transformação Concluída. DataFrame transformado contém:")
    logger.info(f"Dimensões: {df_transformado.shape}")
    logger.info(f"Colunas: {list(df_transformado.columns)}")

    return df_transformado


# --- 3. Etapa de Carregamento ---
def cerregamento_dados(df: pd.DataFrame, file_path: str):
  
    logger.info(f"Iniciando Carregamento de dados para o arquivo: {file_path}")
    
    if df.empty:
        logger.warning("DataFrame vazio. Nenhum dado para carregar.")
        return
        
    try:
        # Carregar para CSV, usando 'utf-8' e separador ';' para compatibilidade
        df.to_csv(file_path, index=False, encoding='utf-8', sep=',')
        logger.info(f"Carregamento Concluído. {len(df)} registros salvos em '{file_path}'.")
    except Exception as e:
        logger.error(f"Erro durante o carregamento do CSV: {e}")


def ler_html():
    html_path = diretorio_do_script / HTML_DATA
    if not os.path.exists(html_path):
        print(f"Arquivo não encontrado: {html_path}")
        return None
    with open(html_path, "r", encoding="utf-8") as f:
        html_content = f.read()
    return html_content

# --- Execução Principal do Pipeline ---
def etl_pipeline():
    logger.info("--- INÍCIO DO PIPELINE ETL ---")
    HTML_DATA = ler_html()
        
    # E - Extração
    produtos = extracao_dados(HTML_DATA)
    
    # T - Transformação
    df_transformado = transformacao_dados(produtos)
    
    # L - Carregamento
    cerregamento_dados(df_transformado, csv_file_path)
    
    logger.info("--- FIM DO PIPELINE ETL ---")

# Executar a função principal
if __name__ == "__main__":
    etl_pipeline()
    
