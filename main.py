import pandas as pd
import re
import json
import requests
from credentials import trello_credentials, supabase_credentials
from supabase import create_client, Client
import logging
import time

# --- CONFIGURAÇÃO DE LOGGING ---
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)

# --- CONFIGURAÇÕES GLOBAIS ---
SUPABASE_TABLE_NAME = "trello_comentarios"
BOARD_IDS = ["kFrWQqjm", "tXcXz9Pl", "WXyXBHeb", "e30OHAsU"]
TRELLO_BASE_URL = "https://api.trello.com/1"


# --- EXTRAÇÃO E NORMALIZAÇÃO DE INFORMAÇÕES ---
def extract_info(text):
    """
    Extrai informações de nome e e-mail de um texto.
    Prioriza a extração de um JSON e, se falhar, usa regex.
    Normaliza os e-mails para minúsculas.
    """
    if not text:
        return []

    data_entries = []

    # 1. Tenta extrair de JSON
    try:
        data = json.loads(text)
        if "dados_socios" in data:
            for socio in data["dados_socios"] or []:
                nome = socio.get("nome")
                for email_dict in socio.get("emails", []):
                    email = email_dict.get("e-mail")
                    if email:
                        data_entries.append({"nome": nome, "email": email.lower()})
        if "emails" in data:
            for email_dict in data["emails"] or []:
                email = email_dict.get("e-mail")
                if email:
                    data_entries.append({"nome": None, "email": email.lower()})
    except json.JSONDecodeError:
        # 2. Se a decodificação JSON falhar, tenta extrair com regex
        email_pattern = r"\b[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}\b"
        found_emails = re.findall(email_pattern, text, re.IGNORECASE)
        for e in found_emails:
            data_entries.append({"nome": None, "email": e.lower()})

    # Remove duplicatas baseadas no e-mail
    seen = set()
    unique_entries = []
    for entry in data_entries:
        if entry["email"] not in seen:
            seen.add(entry["email"])
            unique_entries.append(entry)

    return unique_entries


# --- FUNÇÃO PARA PEGAR TODOS OS COMENTÁRIOS VIA REST ---
def fetch_all_comments(board_id):
    """Busca todos os comentários de um board, usando paginação."""
    all_comments = []
    before = None
    batch_size = 1000  # máximo permitido pela API do Trello

    while True:
        params = {
            "key": trello_credentials["api_key"],
            "token": trello_credentials["token"],
            "filter": "commentCard",
            "limit": batch_size,
        }
        if before:
            params["before"] = before

        url = f"{TRELLO_BASE_URL}/boards/{board_id}/actions"
        try:
            response = requests.get(url, params=params)
            response.raise_for_status()  # Levanta um erro para status de erro (4xx, 5xx)
            batch = response.json()
            if not batch:
                break

            all_comments.extend(batch)
            before = batch[-1]["id"]  # próximo batch
            time.sleep(0.1)  # para não estourar rate limit
        except requests.exceptions.RequestException as e:
            logging.error(f"Erro de requisição ao buscar comentários do board {board_id}: {e}")
            break
        except json.JSONDecodeError:
            logging.error(f"Erro ao decodificar JSON do board {board_id}. Resposta: {response.text}")
            break

    logging.info(f"Total de comentários encontrados no board {board_id}: {len(all_comments)}")
    return all_comments


# --- FUNÇÃO PRINCIPAL ---
def sync_trello_to_supabase():
    """Sincroniza comentários do Trello com a tabela do Supabase."""
    # 1. Inicializa Supabase
    try:
        supabase_client: Client = create_client(
            supabase_credentials["url"], supabase_credentials["key"]
        )
    except Exception as e:
        logging.error(f"Erro ao inicializar Supabase: {e}")
        return

    # 2. Carrega registros existentes para deduplicação
    try:
        existing = supabase_client.from_(SUPABASE_TABLE_NAME).select(
            "id_comentario,email_no_comentario"
        ).execute()
        existing_set = {
            (row["id_comentario"], row["email_no_comentario"]) for row in existing.data
        }
    except Exception as e:
        logging.error(f"Erro ao carregar registros existentes do Supabase: {e}")
        existing_set = set()

    new_rows = []

    # 3. Processa cada board
    for board_id in BOARD_IDS:
        logging.info(f"Iniciando processamento do board: {board_id}")
        comments = fetch_all_comments(board_id)

        for comment in comments:
            comment_id = str(comment.get("id", ""))
            comment_text = comment.get("data", {}).get("text", "")
            comment_date = pd.to_datetime(comment.get("date")).strftime("%Y-%m-%d")
            autor = comment.get("memberCreator", {}).get("fullName", "")

            card = comment.get("data", {}).get("card", {})
            card_id = card.get("id", "Sem ID")
            card_name = card.get("name", "Sem Nome")
            card_url = f"https://trello.com/c/{card.get('shortLink','')}"

            lista = comment.get("data", {}).get("list", {})
            lista_id = lista.get("id", "Sem ID")
            lista_name = lista.get("name", "Sem Lista")

            dados = extract_info(comment_text)
            if not dados:
                logging.debug(f"Comentário {comment_id} ignorado (sem e-mail).")
                continue

            for entry in dados:
                key = (comment_id, entry["email"])
                if key in existing_set:
                    logging.debug(f"Comentário {comment_id} com e-mail {entry['email']} já existe, ignorando.")
                    continue

                new_rows.append(
                    {
                        "id_comentario": comment_id,
                        "id_cartao": card_id,
                        "id_lista": lista_id,
                        "lista": lista_name,
                        "cartao": card_name,
                        "url": card_url,
                        "autor_comentario": autor,
                        "nome_no_comentario": entry["nome"],
                        "email_no_comentario": entry["email"],
                        "data": comment_date,
                        "id_board": board_id,
                    }
                )

    # 4. Inserção em batch no Supabase
    if new_rows:
        try:
            BATCH_SIZE = 500
            for i in range(0, len(new_rows), BATCH_SIZE):
                batch = new_rows[i:i + BATCH_SIZE]
                supabase_client.from_(SUPABASE_TABLE_NAME).insert(batch).execute()
            logging.info(f"Inseridos {len(new_rows)} novos registros.")
        except Exception as e:
            logging.error(f"Erro no insert batch: {e}")
    else:
        logging.info("Nenhum novo registro encontrado.")


if __name__ == "__main__":
    sync_trello_to_supabase()