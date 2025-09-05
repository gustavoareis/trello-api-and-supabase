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

# --- VARIÁVEIS GLOBAIS ---
SUPABASE_TABLE_NAME = "trello_comentarios"
LAST_RUN_FILE = "last_run.txt"

# --- LISTA DOS QUADROS (BOARDS) A SEREM PROCESSADOS ---
BOARD_IDS = [
    'kFrWQqjm',
    'tXcXz9Pl',
    'WXyXBHeb',
    'e30OHAsU'
]

# --- FUNÇÃO DE EXTRAÇÃO QUE CRIA PARES DE NOME-E-MAIL ---
def extract_info(text):
    """
    Extrai pares de nome e e-mail. Retorna uma lista de dicionários,
    onde cada dicionário contém um nome e um e-mail.
    """
    if not text:
        return []

    data_entries = []
    
    # Tenta parsear o texto como JSON
    try:
        data = json.loads(text)
        
        # 1. Extrai pares de nome-e-mail da seção "dados_socios"
        if 'dados_socios' in data and data['dados_socios']:
            for socio_data in data['dados_socios']:
                nome_socio = socio_data.get('nome')
                emails_socio = socio_data.get('emails', [])
                
                # Converte strings vazias em None
                if nome_socio == "":
                    nome_socio = None
                
                if emails_socio:
                    for email_dict in emails_socio:
                        email_value = email_dict.get('e-mail')
                        if email_value:
                            data_entries.append({'nome': nome_socio, 'email': email_value})
                elif nome_socio:
                    data_entries.append({'nome': nome_socio, 'email': None})
        
        # 2. Extrai e-mails da seção "emails" principal (sem nome associado)
        if 'emails' in data and data['emails']:
            for email_dict in data['emails']:
                email_value = email_dict.get('e-mail')
                if email_value:
                    data_entries.append({'nome': None, 'email': email_value})

        # --- DEDUPLICAÇÃO E TRATAMENTO DE VALORES ---
        seen_emails = set()
        unique_entries = []
        for entry in data_entries:
            if entry['email'] not in seen_emails and entry['email'] is not None:
                seen_emails.add(entry['email'])
                unique_entries.append(entry)
            elif entry['email'] is None and not any(e['email'] is None for e in unique_entries):
                unique_entries.append(entry)
        
        return unique_entries

    except json.JSONDecodeError:
        # 3. Lógica para comentários que não são JSON (apenas texto)
        name_pattern = r'\b(?:[A-ZÀ-Ü][a-zà-ü]+(?:\s+e\s+|(?:\s+da)?\s+[A-ZÀ-Ü][a-zà-ü]+))+(?:\s+[A-ZÀ-Ü][a-zà-ü]+)?'
        email_pattern = r'\b[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}\b'
        
        found_names = re.findall(name_pattern, text)
        found_emails = re.findall(email_pattern, text, re.IGNORECASE)
        
        data_entries_text = []
        for email in found_emails:
            data_entries_text.append({'nome': found_names[0] if found_names else None, 'email': email})

        if not found_emails and found_names:
            data_entries_text.append({'nome': found_names[0], 'email': None})
        
        seen_emails = set()
        unique_entries_text = []
        for entry in data_entries_text:
            if entry['email'] not in seen_emails and entry['email'] is not None:
                seen_emails.add(entry['email'])
                unique_entries_text.append(entry)
            elif entry['email'] is None and not any(e['email'] is None for e in unique_entries_text):
                unique_entries_text.append(entry)

        return unique_entries_text


# --- FUNÇÃO PRINCIPAL DE SINCRONIZAÇÃO ---
def sync_trello_to_supabase():
    
    # 1. TRATAMENTO DE ERROS GENÉRICOS DE CONEXÃO E CREDENCIAIS
    try:
        # Inicializa clientes com as credenciais do seu arquivo `credentials.py`
        trello_client = TrelloClient(
            api_key=trello_credentials['api_key'],
            token=trello_credentials['token']
        )
        supabase_client: Client = create_client(
            supabase_credentials['url'],
            supabase_credentials['key']
        )
    except Exception as e:
        print(f"Erro ao inicializar clientes. Verifique suas credenciais em 'credentials.py'. Erro: {e}")
        return

    # 2. SINCRONIZAÇÃO INCREMENTAL
    last_run_timestamp = None
    if os.path.exists(LAST_RUN_FILE):
        with open(LAST_RUN_FILE, 'r') as f:
            last_run_timestamp = f.read().strip()
            print(f"Sincronizando comentários desde: {last_run_timestamp}")
    else:
        print("Primeira execução. Buscando todos os comentários.")

    current_run_timestamp = datetime.now().isoformat()
    
    # --- PROCESSO DE SINCRONIZAÇÃO ---
    print("\nBuscando novos dados em múltiplos quadros do Trello...")
    
    for board_id in BOARD_IDS:
        try:
            board = trello_client.get_board(board_id)
            print(f"\n--- Processando quadro: '{board.name}' (ID: {board_id}) ---")

            for lista in board.all_lists():
                list_id = lista.id
                list_name = lista.name
                
                print(f"--- Processando lista: '{list_name}' (ID: {list_id}) ---")

                for card in lista.list_cards():
                    
                    empresa = card.name 
                    
                    # 3. TRATAMENTO DE ERROS DA API DO TRELLO
                    try:
                        comments = card.fetch_comments()
                        # Filtrar comentários por data (sincronização incremental)
                        if last_run_timestamp:
                            comments = [c for c in comments if c['date'] > last_run_timestamp]
                        
                        if not comments:
                            continue

                        for comment in comments:
                            autor_comentario = comment['memberCreator'].get('fullName', '')
                            texto_comentario = comment['data']['text']
                            dados_encontrados = extract_info(texto_comentario)
                            data_comentario = pd.to_datetime(comment['date']).strftime('%Y-%m-%d')
                            
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

                                # 4. TRATAMENTO DE ERROS DO SUPABASE
                                try:
                                    response = supabase_client.from_(SUPABASE_TABLE_NAME).select('id_comentario').eq('id_comentario', supabase_data['id_comentario']).eq('email_no_comentario', entry['email']).execute()
                                    if response.data:
                                        print(f"Registro com e-mail '{entry['email']}' já existe. Ignorando.")
                                    else:
                                        supabase_client.from_(SUPABASE_TABLE_NAME).insert(supabase_data).execute()
                                        print(f"Novo registro para o e-mail '{entry['email']}' enviado com sucesso.")

                                except Exception as db_e:
                                    print(f"Erro ao interagir com o Supabase para o e-mail '{entry['email']}'. Erro: {db_e}")

                    except requests.exceptions.RequestException as trello_e:
                        print(f"Erro de conexão com a API do Trello. Verifique sua rede ou as credenciais. Erro: {trello_e}")
                    except Exception as e:
                        print(f"Ocorreu um erro ao processar os comentários do cartão '{card.name}'. Erro: {e}")

        except Exception as e:
            print(f"Ocorreu um erro ao processar o quadro com ID '{board_id}': {e}")
    
    # Salva o timestamp da execução atual para a próxima vez
    with open(LAST_RUN_FILE, 'w') as f:
        f.write(current_run_timestamp)
        print(f"\nSincronização concluída! Timestamp da execução salvo em '{LAST_RUN_FILE}'.")

# Executa a função principal
if __name__ == "__main__":
    sync_trello_to_supabase()