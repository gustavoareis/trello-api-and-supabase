import re
import json
import requests
import logging
import time
from datetime import datetime
from credentials import trello_credentials, supabase_credentials
from supabase import create_client, Client

# --- CONFIGURAÇÃO DE LOGGING ---
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)

# --- CONFIGURAÇÕES GLOBAIS ---
SUPABASE_TABLE_NAME = "trello_comentarios"
TRELLO_BASE_URL = "https://api.trello.com/1"

# --- MAPA DE TIPOS DE CAMPANHA POR BOARD/LISTA ---
BOARD_LIST_TIPOS = {
    "kFrWQqjm": {
        "default": "nutrição",
        "listas": {
            "677ee6e1d3a3184d7a9e3a48": "higienização",
        },
    },
    "tXcXz9Pl": {"default": "nutrição"},
    "WXyXBHeb": {"default": "nutrição"},
    "e30OHAsU": {"default": "nutrição"},
}


# --- EXTRAÇÃO E NORMALIZAÇÃO DE INFORMAÇÕES ---
def extract_info(text):
    """Extrai informações de nome e e-mail de um texto.
    Prioriza JSON, fallback para regex.
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
        # 2. Se a decodificação JSON falhar, usa regex
        email_pattern = r"\b[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}\b"
        found_emails = re.findall(email_pattern, text, re.IGNORECASE)
        for e in found_emails:
            data_entries.append({"nome": None, "email": e.lower()})

    # Remove duplicatas
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
    batch_size = 1000  # limite da API Trello

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
            if response.status_code == 429:  # Rate limit
                logging.warning("Rate limit atingido, aguardando 5s...")
                time.sleep(5)
                continue
            response.raise_for_status()
            batch = response.json()
            if not batch:
                break

            all_comments.extend(batch)
            before = batch[-1]["id"]
            time.sleep(0.2)  # para não estourar rate limit
        except requests.exceptions.RequestException as e:
            logging.error(f"Erro ao buscar comentários do board {board_id}: {e}")
            break
        except json.JSONDecodeError:
            logging.error(f"Erro ao decodificar JSON do board {board_id}. Resposta: {response.text}")
            break

    logging.info(f"Total de comentários encontrados no board {board_id}: {len(all_comments)}")
    return all_comments


# --- FUNÇÃO PRINCIPAL ---
def sync_trello_to_supabase():
    """Sincroniza comentários do Trello com a tabela do Supabase."""
    try:
        supabase_client: Client = create_client(
            supabase_credentials["url"], supabase_credentials["key"]
        )
    except Exception as e:
        logging.error(f"Erro ao inicializar Supabase: {e}")
        return

    new_rows = []

    # 2. Processa cada board configurado
    for board_id, regras in BOARD_LIST_TIPOS.items():
        logging.info(f"Iniciando processamento do board: {board_id}")
        comments = fetch_all_comments(board_id)

        for comment in comments:
            comment_id = str(comment.get("id", ""))
            comment_text = comment.get("data", {}).get("text", "")

            # Data segura convertida para string
            date_raw = comment.get("date")
            try:
                comment_date = (
                    datetime.fromisoformat(date_raw.replace("Z", "+00:00")).strftime("%Y-%m-%d")
                    if date_raw else None
                )
            except Exception:
                comment_date = None

            autor = comment.get("memberCreator", {}).get("fullName", "")

            card = comment.get("data", {}).get("card", {})
            card_id = card.get("id", "Sem ID")
            card_name = card.get("name", "Sem Nome")
            card_url = f"https://trello.com/c/{card.get('shortLink','')}"

            lista = comment.get("data", {}).get("list", {})
            lista_id = lista.get("id", "Sem ID")
            lista_name = lista.get("name", "Sem Lista")

            # --- Define tipo de campanha ---
            tipo = regras.get("default")
            tipo = regras.get("listas", {}).get(lista_id, tipo)

            dados = extract_info(comment_text)
            if not dados:
                continue

            for entry in dados:
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
                        "data": comment_date,  # string "YYYY-MM-DD"
                        "id_board": board_id,
                        "tipo_de_campanha": tipo,
                    }
                )

    # 3. Inserção em batch no Supabase com upsert
    if new_rows:
        try:
            BATCH_SIZE = 500
            for i in range(0, len(new_rows), BATCH_SIZE):
                batch = new_rows[i:i + BATCH_SIZE]
                supabase_client.from_(SUPABASE_TABLE_NAME) \
                    .upsert(batch, on_conflict="id_comentario,email_no_comentario") \
                    .execute()
            logging.info(f"Inseridos/atualizados {len(new_rows)} registros.")
        except Exception as e:
            logging.error(f"Erro no insert batch: {e}")
    else:
        logging.info("Nenhum novo registro encontrado.")


if __name__ == "__main__":
    sync_trello_to_supabase()
