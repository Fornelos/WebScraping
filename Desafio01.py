from bs4 import BeautifulSoup
import logging
import pandas as pd
import os


HTML_DATA = """
<div class="product-list">
    <div class="product-card" id="P001" data-date="27/11/2025">
        <h2>Smartphone X10</h2>
        <img src="/img/x10.jpg" alt="X10">
        <p class="price">R$ 1.999,99</p>
        <span class="rating">4.5/5.0</span>
        <span class="discount-rate">10% OFF</span>
    </div>
    <div class="product-card" id="P002" data-date="28/11/2025">
        <h2>Notebook Ultraline</h2>
        <img src="/img/ultra.jpg" alt="Ultra">
        <p class="price">R$ 4.500,00</p>
        <span class="rating">4.8/5.0</span>
        </div>
    <div class="product-card" id="P003" data-date="28/11/2025">
        <h2>Fone Bluetooth Pro</h2>
        <img src="/img/fone.jpg" alt="Fone">
        <p class="price">R$ 549,50</p>
        <span class="rating">3.9/5.0</span>
        <span class="discount-rate">5% OFF</span>
    </div>
    <div class="product-card" id="P004" data-date="27/11/2025">
        <h2>Smartwatch Z</h2>
        <img src="/img/watch.jpg" alt="Watch">
        <p class="price">R$ 1.250,00</p>
        <span class="rating">5.0/5.0</span>
        <span class="discount-rate">20% OFF</span>
    </div>
</div>
"""
OUTPUT_CSV = "C://Nuvem//POS//Mineração de Texto na Web//Desafio1//ofertas_calculadas.csv"
# Definir a pasta onde o log será salvo
LOG_DIR = "C://Nuvem//POS//Mineração de Texto na Web//Desafio1//" # Exemplo de caminho no Windows 
 # Caminho completo do arquivo de log
LOG_FILE_PATH = os.path.join(LOG_DIR, "etl_pipeline.log")


# --- Configuração do Logging ---
logging.basicConfig(level=logging.INFO,
                    format='%(asctime)s - %(levelname)s - %(message)s',
                    handlers=[
                        logging.FileHandler(LOG_FILE_PATH),
                        logging.StreamHandler()
                    ])
logger = logging.getLogger(__name__)


def extract_data(url: str) -> list:

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


# --- 2. Etapa de Transformação (Transform) ---
def transform_data(data: list) -> pd.DataFrame:

    logger.info("Iniciando Transformação de dados.")

    if not data:
        logger.warning("Lista de dados vazia. Pulando a transformação.")
        return pd.DataFrame()
    
    df = pd.DataFrame(data)

    logger.info("Conversão Numérica")
    # Conversão Numérica
    df['preco'] = df['preco'].str.replace('R$', '', regex=True).str.replace('.', '', regex=False).str.replace(',', '.', regex=False)
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
    df_transformed = df[[col for col in final_columns if col in df.columns]]
    
    logger.info("Transformação Concluída. DataFrame transformado contém:")
    logger.info(f"Dimensões: {df_transformed.shape}")
    logger.info(f"Colunas: {list(df_transformed.columns)}")

    return df_transformed 


# --- 3. Etapa de Carregamento (Load) ---
def load_data(df: pd.DataFrame, file_path: str):
    """Carrega o DataFrame transformado em um arquivo CSV."""
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



# --- Execução Principal do Pipeline ---
def run_etl_pipeline():
    logger.info("--- INÍCIO DO PIPELINE ETL ---")
    
    # E - Extração
    produtos = extract_data(HTML_DATA)
    
    # T - Transformação
    transformed_df = transform_data(produtos)
    
    # L - Carregamento
    load_data(transformed_df, OUTPUT_CSV)
    
    logger.info("--- FIM DO PIPELINE ETL ---")

# Executar a função principal
if __name__ == "__main__":
    run_etl_pipeline()
    
