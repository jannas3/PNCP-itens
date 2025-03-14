import requests
import psycopg2
from dotenv import load_dotenv
from datetime import datetime
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.models import Variable

load_dotenv()

# Carregar variáveis de ambiente
load_dotenv()
DATABASE = Variable.get("database")
DATABASE_USER = Variable.get("user")
DATABASE_HOST = Variable.get("host")
DATABASE_PASSWORD = Variable.get("password")
DATABASE_PORT = Variable.get("port")


# Função para conectar ao banco de dados PostgreSQL
def conectar_banco():

    return psycopg2.connect(
        database=DATABASE,
        user=DATABASE_USER,
        host=DATABASE_HOST,
        password=DATABASE_PASSWORD,
        port=DATABASE_PORT,
    )


# API URL e Headers
BASE_URL_CONTRATACAO_ITENS = "https://pncp.gov.br/api/pncp/v1/orgaos"
HEADERS = {"accept": "application/json"}


# 🔹 Função auxiliar para acessar objetos aninhados de forma segura
def get_value_safe(obj, *keys):
    for key in keys:
        if obj is None or not isinstance(obj, dict):
            return None
        obj = obj.get(key)
    return obj


# 🔹 Função para buscar CNPJs do banco de dados
def buscar_cnpjs_banco():
    try:
        conn = conectar_banco()
        if not conn:
            return []

        cursor = conn.cursor()
        query = """
            SELECT DISTINCT numero_controle_pncp, orgao_cnpj, sequencial_compra, ano_compra
            FROM pncp.contratacoes_publicas
            WHERE orgao_esfera_id <> 'F';
        """
        cursor.execute(query)
        resultados = cursor.fetchall()

        cursor.close()
        conn.close()

        return resultados

    except Exception as e:
        print(f"Erro ao buscar CNPJs do banco: {e}")
        return []


#  Buscar Itens de Contratação na API
def buscar_itens_por_cnpj_ano_sequencial(cnpj, ano, sequencial):
    itens_coletados = []
    numero_item = 1

    while True:
        url = f"{BASE_URL_CONTRATACAO_ITENS}/{cnpj}/compras/{ano}/{sequencial}/itens/{numero_item}"
        response = requests.get(url, headers=HEADERS)

        if response.status_code == 200:
            item = response.json()
            itens_coletados.append(item)
            print(
                f"Item {numero_item} coletado para CNPJ {cnpj} - Sequencial {sequencial}"
            )
        elif response.status_code == 404:
            print(
                f"Item {numero_item} não encontrado. Fim dos itens para este sequencial."
            )
            break
        else:
            print(f"Erro na requisição {numero_item}: {response.status_code}")
            break

        numero_item += 1

    return itens_coletados


# Função para inserir registros no banco de dados
def inserir_dados_banco(dados):
    conexao = conectar_banco()
    if not conexao:
        return

    cursor = conexao.cursor()
    registros_para_inserir = []

    # Validar e preparar registros
    for registro in dados:
        numero_controle_pncp = str(get_value_safe(registro, "numero_controle_pncp"))
        numero_item = str(get_value_safe(registro, "numeroItem"))

        if not numero_controle_pncp or not numero_item:
            print(
                "❗ Registro inválido: 'numero_controle_pncp' ou 'numeroItem' ausente."
            )
            continue

        registros_para_inserir.append((numero_controle_pncp, numero_item, registro))

    if not registros_para_inserir:
        print("⚠ Nenhum registro válido para inserir.")
        cursor.close()
        conexao.close()
        return

    # Agora vamos verificar registros já existentes no banco!
    # Coleta dos numero_controle_pncp únicos
    controles_unicos = tuple(set([r[0] for r in registros_para_inserir]))

    registros_existentes = set()

    try:
        # Montando a query dinâmica para o IN
        if len(controles_unicos) == 1:
            controles_unicos = (
                controles_unicos[0],
                controles_unicos[0],
            )  # Evita erro no psycopg2 com um único valor

        placeholders = ",".join(["%s"] * len(controles_unicos))

        query = f"""
            SELECT numero_controle_pncp, numeroItem
            FROM pncp.contratacao_itens_pncp
            WHERE numero_controle_pncp IN ({placeholders})
        """

        cursor.execute(query, controles_unicos)

        resultados_existentes = cursor.fetchall()
        registros_existentes = {
            (str(res[0]), str(res[1])) for res in resultados_existentes
        }

        print(f"✔ {len(registros_existentes)} registros já existem no banco.")

    except Exception as e:
        print(f"❗ Erro ao verificar registros existentes: {e}")

    # Inserção dos registros que não existem
    for numero_controle_pncp, numero_item, registro in registros_para_inserir:
        if (numero_controle_pncp, numero_item) in registros_existentes:
            print(
                f"⚠ Item {numero_item} do controle {numero_controle_pncp} já existe. Pulando."
            )
            continue
        try:
            cursor.execute(
                """
                INSERT INTO pncp.contratacao_itens_pncp (
                 numero_controle_pncp, orgao_cnpj, sequencial_compra, numeroItem,descricao, materialOuServico, materialOuServicoNome,valorUnitarioEstimado, valorTotal, quantidade, unidadeMedida, orcamentoSigiloso,
                  itemCategoriaId, itemCategoriaNome, patrimonio, codigoRegistroImobiliario, criterioJulgamentoId, criterioJulgamentoNome, situacaoCompraItem,
                  situacaoCompraItemNome, tipoBeneficio, tipoBeneficioNome, incentivoProdutivoBasico, dataInclusao, dataAtualizacao, temResultado, imagem, 
                  aplicabilidadeMargemPreferenciaNormal,aplicabilidadeMargemPreferenciaAdicional,percentualMargemPreferenciaNormal,percentualMargemPreferenciaAdicional,
                  ncmNbsCodigo,ncmNbsDescricao,catalogo,categoriaItemCatalogo,catalogoCodigoItem,informacaoComplementar
                )
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                        %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                        %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                        %s,%s,%s,%s,%s,%s,%s
                        )
                        ON CONFLICT (numero_controle_pncp, numeroitem) DO NOTHING;
                """,
                (
                    get_value_safe(registro, "numero_controle_pncp"),
                    get_value_safe(registro, "orgao_cnpj"),
                    get_value_safe(registro, "sequencial_compra"),
                    get_value_safe(registro, "numeroItem"),
                    get_value_safe(registro, "descricao"),
                    get_value_safe(registro, "materialOuServico"),
                    get_value_safe(registro, "materialOuServicoNome"),
                    get_value_safe(registro, "valorUnitarioEstimado"),
                    get_value_safe(registro, "valorTotal"),
                    get_value_safe(registro, "quantidade"),
                    get_value_safe(registro, "unidadeMedida"),
                    get_value_safe(registro, "orcamentoSigiloso"),
                    get_value_safe(registro, "itemCategoriaId"),
                    get_value_safe(registro, "itemCategoriaNome"),
                    get_value_safe(registro, "patrimonio"),
                    get_value_safe(registro, "codigoRegistroImobiliario"),
                    get_value_safe(registro, "criterioJulgamentoId"),
                    get_value_safe(registro, "criterioJulgamentoNome"),
                    get_value_safe(registro, "situacaoCompraItem"),
                    get_value_safe(registro, "situacaoCompraItemNome"),
                    get_value_safe(registro, "tipoBeneficio"),
                    get_value_safe(registro, "tipoBeneficioNome"),
                    get_value_safe(registro, "incentivoProdutivoBasico"),
                    get_value_safe(registro, "dataInclusao"),
                    get_value_safe(registro, "dataAtualizacao"),
                    get_value_safe(registro, "temResultado"),
                    get_value_safe(registro, "imagem"),
                    get_value_safe(registro, "aplicabilidadeMargemPreferenciaNormal"),
                    get_value_safe(
                        registro, "aplicabilidadeMargemPreferenciaAdicional"
                    ),
                    get_value_safe(registro, "percentualMargemPreferenciaNormal"),
                    get_value_safe(registro, "percentualMargemPreferenciaAdicional"),
                    get_value_safe(registro, "ncmNbsCodigo"),
                    get_value_safe(registro, "ncmNbsDescricao"),
                    get_value_safe(registro, "catalogo"),
                    get_value_safe(registro, "categoriaItemCatalogo"),
                    get_value_safe(registro, "catalogoCodigoItem"),
                    get_value_safe(registro, "informacaoComplementar"),
                ),
            )
        except Exception as e:
            conexao.rollback()
            print(f"Erro ao inserir registro {numero_item}: {e}")

    conexao.commit()
    cursor.close()
    conexao.close()

def importarContratacaoItensPNCP()
# Função que processa todos os CNPJs + Sequenciais + Anos
def processar_todos_cnpjs():
    resultados = buscar_cnpjs_banco()

    if not resultados:
        print("Nenhum CNPJ, sequencial ou ano encontrado no banco.")
        return
    total_cnpjs = len(resultados)
    print(f"🔢 Total de {total_cnpjs} CNPJs encontrados para processar.")

    for numero_controle_pncp, cnpj, sequencial, ano in resultados:
        print(
            f"🔄 Buscando itens para CNPJ: {cnpj}, Ano: {ano}, Sequencial: {sequencial}"
        )
        itens = buscar_itens_por_cnpj_ano_sequencial(cnpj, ano, sequencial)

        if itens:
            print(
                f"✅ {len(itens)} itens coletados para CNPJ {cnpj} - Sequencial {sequencial}"
            )

            # Adiciona numero_controle_pncp e orgao_cnpj a cada item
            itens_completos = []
            for item in itens:
                item["numero_controle_pncp"] = numero_controle_pncp
                item["orgao_cnpj"] = cnpj
                item["sequencial_compra"] = str(sequencial)
                itens_completos.append(item)

            inserir_dados_banco(itens_completos)
        else:
            print(
                f"⚠️ Nenhum item encontrado para CNPJ {cnpj} - Sequencial {sequencial}"
            )


if __name__ == "__main__":
    print("🔵 Iniciando o processamento completo...")
    processar_todos_cnpjs()
    print("✅ Processamento concluído!")


default_args = {"owner": "airflow", "start_date": datetime(2021, 1, 1)}

# Definir o objeto DAG
dag = DAG(
    "dagImportarContratacaoItensPNCP",
    default_args=default_args,
    description="Importa itens de contratação do PNCP para o banco de dados local",
    schedule_interval=None,
    max_active_runs=1,
)

# Definir a tarefa PythonOperator
dagImportarContratosPNCP = PythonOperator(
    task_id="dagImportarContratacaoItensPNCP", python_callable=importarContratacaoItensPNCP, dag=dag
)

dagImportarContratacaoItensPNCP