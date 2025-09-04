import pandas as pd
from trello import TrelloClient
import re
from unidecode import unidecode
from credentials import trello_credentials, supabase_credentials
from supabase import create_client, Client

# --- CONFIGURAÇÕES GLOBAIS ---
LISTA_ALVO = "CADÊNCIA DE NUTRIÇÃO"
MEMBRO_ALVO = "samantha"
SUPABASE_TABLE_NAME = "trello_comentarios"

# --- INICIALIZA CLIENTES ---
client = TrelloClient(
    api_key=trello_credentials['api_key'],
    token=trello_credentials['token']
)

supabase: Client = create_client(
    supabase_credentials['url'],
    supabase_credentials['key']
)

# --- FUNÇÃO DE EXTRAÇÃO ---
def extract_info(text):
    """Extrai nomes (em maiúsculas) e e-mails do texto do comentário."""
    if not text:
        return None, None

    name_pattern = r'"([A-ZÀ-Ü][A-ZÀ-Ü\s]+)"|\b[A-ZÀ-Ü]{2,}(?:\s+[A-ZÀ-Ü]{2,})+\b'
    email_pattern = r'\b[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}\b'
    
    name_match = re.search(name_pattern, text)
    email_match = re.search(email_pattern, text, re.IGNORECASE)

    extracted_name = name_match.group(1).strip() if name_match and name_match.group(1) else (name_match.group(0).strip() if name_match else None)
    extracted_email = email_match.group(0).lower() if email_match else None

    return extracted_name, extracted_email

# --- PROCESSA COMENTÁRIOS E SINCRONIZA COM SUPABASE ---
try:
    board = client.get_board('e30OHAsU')
    print("\n🔎 Buscando novos dados no Trello...")

    lista_alvo = next((l for l in board.all_lists() if unidecode(l.name.lower()) == unidecode(LISTA_ALVO.lower())), None)

    if not lista_alvo:
        print(f"❌ Erro: Lista '{LISTA_ALVO}' não encontrada no quadro.")
    else:
        for card in lista_alvo.list_cards():
            for comment in card.fetch_comments():
                autor = unidecode(comment['memberCreator'].get('fullName', '').lower())
                texto_comentario = comment['data']['text']

                if MEMBRO_ALVO in autor or MEMBRO_ALVO in unidecode(texto_comentario.lower()):
                    
                    nome, email = extract_info(texto_comentario)
                    
                    data_comentario = pd.to_datetime(comment['date']).strftime('%Y-%m-%d')
                    
                    # Prepara o dicionário de dados
                    supabase_data = {
                        "id_comentario": str(comment['id']),
                        "id_cartao": str(card.id),
                        "lista": str(lista_alvo.name),
                        "cartao": str(card.name),
                        "url": str(card.url),
                        "autor_comentario": str(comment['memberCreator']['fullName']),
                        "nome_no_comentario": nome,
                        "email_no_comentario": email,
                        "data": data_comentario
                    }
                    
                    # 1. Tenta encontrar o registro pelo id_comentario
                    response = supabase.from_(SUPABASE_TABLE_NAME).select('id_comentario').eq('id_comentario', supabase_data['id_comentario']).execute()

                    if response.data:
                        # 2. Se o registro existe, faz um UPDATE
                        response = supabase.from_(SUPABASE_TABLE_NAME).update(supabase_data).eq('id_comentario', supabase_data['id_comentario']).execute()
                        if response.data:
                            print(f"🔄 Registro para o comentário '{comment['id']}' atualizado com sucesso.")
                        else:
                            print(f"\n❌ Erro ao atualizar registro (ID: {comment['id']}): {response.error}")
                    else:
                        # 3. Se o registro NÃO existe, faz um INSERT
                        response = supabase.from_(SUPABASE_TABLE_NAME).insert(supabase_data).execute()
                        if response.data:
                            print(f"✅ Novo registro para o comentário '{comment['id']}' enviado com sucesso.")
                        else:
                            print(f"\n❌ Erro ao inserir novo registro (ID: {comment['id']}): {response.error}")

except Exception as e:
    print(f"\n❌ Ocorreu um erro geral durante a execução: {e}")

print("\n✅ Sincronização concluída!")