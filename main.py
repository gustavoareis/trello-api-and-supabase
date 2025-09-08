import pandas as pd
from trello import TrelloClient 
import re 
import json 
from unidecode import unidecode 
from credentials import trello_credentials, supabase_credentials
from supabase import create_client, Client 
import requests.exceptions 
import os 
from datetime import datetime

# --- Configurações globais ---

# Nome da tabela no Supabase onde os dados serão armazenados.
SUPABASE_TABLE_NAME = "trello_comentarios"
# Nome do arquivo que armazena a data e hora da última execução do script.
LAST_RUN_FILE = "last_run.txt"

# Lista de IDs dos quadros do Trello a serem processados.
BOARD_IDS = [
    'kFrWQqjm',
    'tXcXz9Pl',
    'WXyXBHeb',
    'e30OHAsU'
]

# --- Funções ---

def extract_info(text):
    """
    Extrai nomes e e-mails de um texto. O texto pode ser uma string JSON ou texto simples.
    """
    if not text:
        return []

    data_entries = []
    
    try:
        # Tenta carregar o texto como JSON.
        data = json.loads(text)
        
        # Extrai e-mails de dados de sócios.
        if 'dados_socios' in data and data['dados_socios']:
            for socio_data in data['dados_socios']:
                nome_socio = socio_data.get('nome') or None
                emails_socio = socio_data.get('emails', [])
                
                if emails_socio:
                    for email_dict in emails_socio:
                        email_value = email_dict.get('e-mail')
                        if email_value:
                            data_entries.append({'nome': nome_socio, 'email': email_value})
        
        # Extrai e-mails de uma lista direta no JSON.
        if 'emails' in data and data['emails']:
            for email_dict in data['emails']:
                email_value = email_dict.get('e-mail')
                if email_value:
                    data_entries.append({'nome': None, 'email': email_value})

        # Remove e-mails duplicados para garantir entradas únicas.
        seen_emails = set()
        unique_entries = []
        for entry in data_entries:
            if entry['email'] not in seen_emails and entry['email'] is not None:
                seen_emails.add(entry['email'])
                unique_entries.append(entry)
        
        return unique_entries

    except json.JSONDecodeError:
        # Se não for JSON, usa expressões regulares para extrair os dados.
        name_pattern = r'\b(?:[A-ZÀ-Ü][a-zà-ü]+(?:\s+e\s+|(?:\s+da)?\s+[A-ZÀ-Ü][a-zà-ü]+))+(?:\s+[A-ZÀ-Ü][a-zà-ü]+)?'
        email_pattern = r'\b[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}\b'
        
        found_names = re.findall(name_pattern, text)
        found_emails = re.findall(email_pattern, text, re.IGNORECASE)
        
        data_entries_text = []
        seen_emails = set()
        for email in found_emails:
            if email not in seen_emails:
                data_entries_text.append({'nome': found_names[0] if found_names else None, 'email': email})
                seen_emails.add(email)

        return data_entries_text


def sync_trello_to_supabase():
    """
    Sincroniza comentários do Trello com o Supabase.
    """
    
    try:
        # Inicializa os clientes Trello e Supabase.
        trello_client = TrelloClient(api_key=trello_credentials['api_key'], token=trello_credentials['token'])
        supabase_client: Client = create_client(supabase_credentials['url'], supabase_credentials['key'])
    except Exception as e:
        print(f"Erro ao inicializar clientes. Verifique suas credenciais. Erro: {e}")
        return

    # Lê a data da última execução para buscar apenas comentários novos.
    last_run_timestamp = None
    if os.path.exists(LAST_RUN_FILE):
        with open(LAST_RUN_FILE, 'r') as f:
            last_run_timestamp = f.read().strip()
    
    current_run_timestamp = datetime.now().isoformat()
    
    print("\nBuscando novos dados em múltiplos quadros do Trello...")
    
    # Itera sobre cada quadro do Trello configurado.
    for board_id in BOARD_IDS:
        try:
            board = trello_client.get_board(board_id)
            print(f"Processando quadro: '{board.name}'")

            # Itera sobre todas as listas e cartões dentro do quadro.
            for lista in board.all_lists():
                list_id = lista.id
                list_name = lista.name
                
                for card in lista.list_cards():
                    
                    empresa = card.name 
                    
                    try:
                        # Busca os comentários do cartão e filtra por data.
                        comments = card.fetch_comments()
                        if last_run_timestamp:
                            comments = [c for c in comments if c['date'] > last_run_timestamp]
                        
                        if not comments:
                            continue

                        for comment in comments:
                            autor_comentario = comment['memberCreator'].get('fullName', '')
                            texto_comentario = comment['data']['text']
                            dados_encontrados = extract_info(texto_comentario)
                            
                            if not dados_encontrados:
                                continue

                            data_comentario = pd.to_datetime(comment['date']).strftime('%Y-%m-%d')
                            
                            # Para cada e-mail encontrado, prepara e insere os dados no Supabase.
                            for entry in dados_encontrados:
                                supabase_data = {
                                    "id_comentario": str(comment['id']),
                                    "id_cartao": str(card.id),
                                    "id_lista": str(list_id), 
                                    "lista": str(list_name),
                                    "cartao": str(empresa),
                                    "url": str(card.url),
                                    "autor_comentario": str(autor_comentario),
                                    "nome_no_comentario": entry['nome'],
                                    "email_no_comentario": entry['email'],
                                    "data": data_comentario
                                }

                                try:
                                    # Verifica se o registro já existe antes de inserir para evitar duplicatas.
                                    response = supabase_client.from_(SUPABASE_TABLE_NAME).select('id_comentario').eq('id_comentario', supabase_data['id_comentario']).eq('email_no_comentario', entry['email']).execute()
                                    if not response.data:
                                        supabase_client.from_(SUPABASE_TABLE_NAME).insert(supabase_data).execute()
                                except Exception as db_e:
                                    print(f"Erro ao interagir com o Supabase. Erro: {db_e}")

                    except requests.exceptions.RequestException as trello_e:
                        print(f"Erro de conexão com o Trello. Erro: {trello_e}")
                    except Exception as e:
                        print(f"Erro ao processar o cartão '{card.name}'. Erro: {e}")

        except Exception as e:
            print(f"Erro ao processar o quadro com ID '{board_id}': {e}")
    
    # Salva o timestamp da execução atual para ser usado na próxima rodada.
    with open(LAST_RUN_FILE, 'w') as f:
        f.write(current_run_timestamp)

if __name__ == "__main__":
    sync_trello_to_supabase()