import pandas as pd
from trello import TrelloClient
import re
from unidecode import unidecode
from credentials import trello_credentials, supabase_credentials
from supabase import create_client, Client

# --- CONFIGURAÇÕES TRELLO ---
LISTA_ALVO = "CADÊNCIA DE NUTRIÇÃO"
MEMBRO_ALVO = "samantha"

# --- CREDENCIAIS TRELLO ---
client = TrelloClient(
    api_key=trello_credentials['api_key'],
    token=trello_credentials['token']
)

# --- CREDENCIAIS SUPABASE ---
supabase: Client = create_client(
    supabase_credentials['url'],
    supabase_credentials['key']
)
supabase_table_name = "trello_comentarios"


def extract_info(text):
    """Extrai nomes (em maiúsculas) e e-mails do texto"""
    if not text:
        return [], []

    name_pattern = r'"[A-ZÀ-Ü\s]+"|\b[A-ZÀ-Ü][A-ZÀ-Ü\s]{3,}\b'
    email_pattern = r'\b[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}\b'
    
    names = re.findall(name_pattern, text)
    emails = re.findall(email_pattern, text, re.IGNORECASE)

    cleaned_names = [name.strip('"').strip() for name in names if len(name.split()) >= 2]
    cleaned_emails = list(set([email.lower() for email in emails]))

    return cleaned_names, cleaned_emails

# --- PASSO 1: BUSCAR E-MAILS JÁ EXISTENTES NO BANCO DE DADOS ---
print("🔍 Verificando e-mails existentes no Supabase...")
try:
    response = supabase.table(supabase_table_name).select('email_no_comentario').execute()
    # Cria um conjunto (set) para uma verificação rápida e eficiente
    existing_emails = {item['email_no_comentario'] for item in response.data}
    print(f"✅ {len(existing_emails)} e-mails encontrados no banco de dados.")
except Exception as e:
    print(f"🚨 ERRO ao buscar e-mails existentes: {e}")
    existing_emails = set() # Continua com um conjunto vazio em caso de erro

# --- INÍCIO DA COLETA DE DADOS DO TRELLO ---
board = client.get_board('e30OHAsU')
dados = []

print("\n🔎 Buscando novos dados no Trello...")

for lista in board.all_lists():
    if unidecode(lista.name.lower()) == unidecode(LISTA_ALVO.lower()):
        for card in lista.list_cards():
            for comment in card.fetch_comments():
                autor = unidecode(comment['memberCreator'].get('fullName', '').lower())
                texto_comentario = comment['data']['text']

                if MEMBRO_ALVO in autor or MEMBRO_ALVO in unidecode(texto_comentario.lower()):
                    nomes, emails = extract_info(texto_comentario)

                    if emails:
                        emails_a_remover = set()
                        for email1 in emails:
                            for email2 in emails:
                                if email1 != email2 and email1 in email2:
                                    emails_a_remover.add(email1)
                        
                        emails_filtrados = [e for e in emails if e not in emails_a_remover]
                        
                        if emails_filtrados:
                            nome_identificado = nomes[0] if nomes else "NÃO IDENTIFICADO"
                            
                            for email in emails_filtrados:
                                dados.append({
                                    'ID do Cartão': card.id,
                                    'Lista': lista.name,
                                    'Cartão': card.name,
                                    'URL': card.url,
                                    'Autor do Comentário': comment['memberCreator']['fullName'],
                                    'Nome no Comentário': nome_identificado,
                                    'E-mail no Comentário': email,
                                    'Data': comment['date']
                                })

if dados:
    df = pd.DataFrame(dados)
    # Remove duplicatas coletadas NESTA EXECUÇÃO
    df_depois_trello = df.drop_duplicates(subset=['E-mail no Comentário'], keep='first')

    # Mapeia as colunas ANTES de filtrar para manter o padrão
    colunas_mapeadas = {
        'ID do Cartão': 'id_cartao',
        'Lista': 'lista',
        'Cartão': 'cartao',
        'URL': 'url',
        'Autor do Comentário': 'autor_comentario',
        'Nome no Comentário': 'nome_no_comentario',
        'E-mail no Comentário': 'email_no_comentario',
        'Data': 'data'
    }
    df_final = df_depois_trello.rename(columns=colunas_mapeadas)

    # --- PASSO 2: FILTRAR OS NOVOS DADOS, REMOVENDO OS QUE JÁ EXISTEM NO BANCO ---
    print(f"\n⚙️  {len(df_final)} e-mails únicos encontrados no Trello.")
    # A mágica acontece aqui: `isin(existing_emails)` checa quais e-mails já existem.
    # O `~` na frente inverte a seleção, pegando apenas os que NÃO existem.
    df_para_inserir = df_final[~df_final['email_no_comentario'].isin(existing_emails)]
    
    # Verifica se há algo novo para inserir
    if not df_para_inserir.empty:
        df_para_inserir['data'] = pd.to_datetime(df_para_inserir['data']).dt.strftime('%Y-%m-%d')
        dados_para_inserir = df_para_inserir.to_dict(orient='records')

        print(f"✨ {len(dados_para_inserir)} NOVOS registros encontrados para adicionar.")
        print("☁️  Enviando dados para o Supabase...")

        try:
            response = supabase.table(supabase_table_name).insert(dados_para_inserir).execute()

            if len(response.data) > 0:
                print("\n✅ DADOS ENVIADOS COM SUCESSO!")
                print(f"📝 Total de registros inseridos: {len(response.data)}")
            else:
                print("\n❌ FALHA AO ENVIAR DADOS.")
                if hasattr(response, 'error') and response.error:
                    print("Detalhes do erro:", response.error)
        
        except Exception as e:
            print(f"\n🚨 OCORREU UM ERRO DURANTE A INSERÇÃO NO BANCO DE DADOS:")
            print(e)
    else:
        print("\n✅ NENHUM e-mail novo encontrado. O banco de dados já está atualizado.")

else:
    print("\n❌ NENHUM DADO ENCONTRADO NO TRELLO COM OS CRITÉRIOS ESPECIFICADOS.")