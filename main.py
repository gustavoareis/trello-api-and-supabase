# main.py
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

# --- CACHES GLOBAIS ---
list_cache = {}             # {list_id: {"id","name","closed"}}
board_lists_loaded = set()  # boards com listas já pré-carregadas
card_index_cache = {}       # {board_id: {card_id: {"idList","closed","name","shortLink"}}}
board_info_cache = {}       # {board_id: {"id","name","closed"}}

# -------------------------
# HELPERS DE REDE / TRELLO
# -------------------------
def trello_get(url, params, tries=3, sleep_seconds=0.2):
    """
    GET com tratamento de rate-limit (429) e tentativas com backoff leve.
    """
    for attempt in range(1, tries + 1):
        try:
            r = requests.get(url, params=params, timeout=60)
            if r.status_code == 429:
                wait = 5 if attempt < tries else 8
                logging.warning(f"Rate limit Trello (429). Tentativa {attempt}/{tries}. Aguardando {wait}s...")
                time.sleep(wait)
                continue
            r.raise_for_status()
            return r
        except requests.RequestException as e:
            if attempt >= tries:
                raise
            backoff = sleep_seconds * (attempt + 1)
            logging.warning(f"Falha GET {url} (tentativa {attempt}/{tries}): {e}. Retry em {backoff:.1f}s...")
            time.sleep(backoff)
    raise RuntimeError("trello_get: esgotadas tentativas")

def get_board_info(board_id):
    """
    Busca info mínima do board (inclui 'closed') e cacheia.
    """
    if board_id in board_info_cache:
        return board_info_cache[board_id]
    url = f"{TRELLO_BASE_URL}/boards/{board_id}"
    params = {
        "key": trello_credentials["api_key"],
        "token": trello_credentials["token"],
        "fields": "id,name,closed",
    }
    try:
        r = trello_get(url, params)
        data = r.json() or {}
        info = {
            "id": data.get("id", board_id),
            "name": data.get("name", ""),
            "closed": bool(data.get("closed", False)),
        }
        board_info_cache[board_id] = info
        return info
    except Exception as e:
        logging.warning(f"Falha ao obter board {board_id}: {e}")
        return {"id": board_id, "name": "", "closed": False}

def preload_lists_for_board(board_id):
    """
    Carrega TODAS as listas (abertas e arquivadas) do board e povoa list_cache.
    """
    if board_id in board_lists_loaded:
        return
    url = f"{TRELLO_BASE_URL}/boards/{board_id}/lists"
    params = {
        "key": trello_credentials["api_key"],
        "token": trello_credentials["token"],
        "filter": "all",  # inclui arquivadas
        "fields": "id,name,closed",
    }
    try:
        r = trello_get(url, params)
        lists = r.json() or []
        for lst in lists:
            list_cache[lst["id"]] = {
                "id": lst["id"],
                "name": lst.get("name", "Sem Lista"),
                "closed": bool(lst.get("closed", False)),
            }
        board_lists_loaded.add(board_id)
        logging.info(f"Listas pré-carregadas do board {board_id}: {len(lists)}")
        time.sleep(0.1)
    except Exception as e:
        logging.error(f"Erro ao pré-carregar listas do board {board_id}: {e}")

def preload_cards_for_board(board_id, only_ids=None):
    """
    Carrega TODOS os cartões (abertos e arquivados) do board em um único request
    e cria índice {card_id: {"idList","closed","name","shortLink"}}.
    Se only_ids for fornecido, mantém só os necessários (economiza RAM).
    """
    url = f"{TRELLO_BASE_URL}/boards/{board_id}/cards"
    params = {
        "key": trello_credentials["api_key"],
        "token": trello_credentials["token"],
        "filter": "all",  # inclui arquivados
        "fields": "id,name,shortLink,idList,closed",
    }
    r = trello_get(url, params)
    cards = r.json() or []

    only = set(only_ids) if only_ids else None
    index = {}
    for c in cards:
        cid = c.get("id")
        if not cid:
            continue
        if only and cid not in only:
            continue
        index[cid] = {
            "idList": c.get("idList"),
            "closed": bool(c.get("closed")),
            "name": c.get("name"),
            "shortLink": c.get("shortLink"),
        }
    card_index_cache[board_id] = index
    logging.info(f"Cartões pré-carregados do board {board_id}: {len(index)} (filtrados)")
    time.sleep(0.1)
    return index

def get_list_info(list_id):
    """
    Retorna {"id","name","closed"} do cache; se faltar, busca uma vez.
    """
    if not list_id:
        return None
    info = list_cache.get(list_id)
    if info:
        return info
    # fallback (raro)
    url = f"{TRELLO_BASE_URL}/lists/{list_id}"
    params = {
        "key": trello_credentials["api_key"],
        "token": trello_credentials["token"],
        "fields": "id,name,closed",
    }
    try:
        r = trello_get(url, params)
        lst = r.json() or {}
        info = {
            "id": lst.get("id", list_id),
            "name": lst.get("name", "Sem Lista"),
            "closed": bool(lst.get("closed", False)),
        }
        list_cache[list_id] = info
        time.sleep(0.1)
        return info
    except Exception as e:
        logging.warning(f"Falha ao obter lista {list_id}: {e}")
        return {"id": list_id, "name": "Sem Lista", "closed": False}

# -----------------------------------------
# EXTRAÇÃO / NORMALIZAÇÃO DE INFORMAÇÕES
# -----------------------------------------
def extract_info(text):
    """Extrai nome, e-mail, telefone/whatsapp de um texto. Marca se é de sócio."""
    if not text:
        return []
    data_entries = []
    email_pattern = r"\b[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}\b"
    telefone_pattern = r"\b(?:\+?55\s?)?(?:\(?\d{2}\)?\s?)?\d{4,5}[- ]?\d{4}\b"
    # 1) JSON estruturado
    try:
        data = json.loads(text)
        if "dados_socios" in data:
            for socio in data["dados_socios"] or []:
                nome = socio.get("nome")
                for email_dict in socio.get("emails", []):
                    email = email_dict.get("e-mail")
                    if email:
                        data_entries.append({
                            "nome": nome, "email": email.lower(),
                            "telefone": None, "whatsapp": None, "is_socio": True
                        })
                for tel_dict in socio.get("telefones", []):
                    telefone = tel_dict.get("telefone")
                    if telefone:
                        telefone_num = re.sub(r"\D", "", telefone)
                        is_whatsapp = (len(telefone_num) >= 11 and telefone_num[-9] == "9")
                        data_entries.append({
                            "nome": nome, "email": None,
                            "telefone": telefone_num,
                            "whatsapp": telefone_num if is_whatsapp else None,
                            "is_socio": True
                        })
        if "emails" in data:
            for email_dict in data["emails"] or []:
                email = email_dict.get("e-mail")
                if email:
                    data_entries.append({
                        "nome": None, "email": email.lower(),
                        "telefone": None, "whatsapp": None, "is_socio": False
                    })
    except json.JSONDecodeError:
        # 2) Regex
        for e in re.findall(email_pattern, text, re.IGNORECASE):
            data_entries.append({
                "nome": None, "email": e.lower(),
                "telefone": None, "whatsapp": None, "is_socio": False
            })
        for t in re.findall(telefone_pattern, text):
            telefone_num = re.sub(r"\D", "", t)
            is_whatsapp = (len(telefone_num) >= 11 and telefone_num[-9] == "9")
            data_entries.append({
                "nome": None, "email": None,
                "telefone": telefone_num,
                "whatsapp": telefone_num if is_whatsapp else None,
                "is_socio": False
            })
    # dedupe interno do comentário
    seen = set()
    unique_entries = []
    for entry in data_entries:
        key = f"{entry.get('email')}-{entry.get('telefone')}-{entry.get('whatsapp')}-{entry.get('is_socio')}"
        if key not in seen:
            seen.add(key)
            unique_entries.append(entry)
    return unique_entries

# ----------------------
# DEDUPE / UPSERT KEYS
# ----------------------
def _conflict_key(row):
    """Chave de conflito (id_comentario, email, telefone) normalizada."""
    return (
        row["id_comentario"],
        (row.get("email_no_comentario") or "").lower(),
        row.get("telefone") or "",
    )

def dedupe_for_upsert(rows):
    """
    Dedupe pela chave de conflito.
    Preferência: whatsapp preenchido > data mais recente > última ocorrência.
    """
    keep = {}
    for i, r in enumerate(rows):
        key = _conflict_key(r)
        score = (
            1 if (r.get("whatsapp") not in (None, "")) else 0,
            (r.get("data") or ""),
            i,
        )
        prev = keep.get(key)
        if (prev is None) or (score > prev["score"]):
            keep[key] = {"row": r, "score": score}
    return [v["row"] for v in keep.values()]

# ----------------------------
# COLETA DE COMENTÁRIOS
# ----------------------------
def fetch_all_comments(board_id):
    """Busca todos os comentários de um board, com paginação."""
    all_comments, before = [], None
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
            r = trello_get(url, params)
            batch = r.json()
            if not batch:
                break
            all_comments.extend(batch)
            before = batch[-1]["id"]
            time.sleep(0.2)
        except json.JSONDecodeError:
            logging.error(f"Erro ao decodificar JSON do board {board_id}.")
            break
        except Exception as e:
            logging.error(f"Erro ao buscar comentários do board {board_id}: {e}")
            break
    logging.info(f"Total de comentários encontrados no board {board_id}: {len(all_comments)}")
    return all_comments

# ----------------------------
# PIPELINE PRINCIPAL
# ----------------------------
def sync_trello_to_supabase():
    """Sincroniza comentários com Supabase, ignorando TUDO que estiver arquivado (board/lista/cartão)."""
    try:
        supabase_client: Client = create_client(
            supabase_credentials["url"], supabase_credentials["key"]
        )
    except Exception as e:
        logging.error(f"Erro ao inicializar Supabase: {e}")
        return

    new_rows = []

    for board_id, regras in BOARD_LIST_TIPOS.items():
        # 0) Se o board está arquivado, pula tudo
        board_info = get_board_info(board_id)
        if board_info.get("closed"):
            logging.info(f"Board {board_id} está arquivado — ignorado.")
            continue

        logging.info(f"Iniciando processamento do board: {board_id}")

        # 1) Pré-carregar listas (inclui arquivadas — vamos filtrar depois)
        preload_lists_for_board(board_id)

        # 2) Puxar comentários
        comments = fetch_all_comments(board_id)
        total = len(comments)

        # 3) Descobrir quais cartões estão nos comentários e pré-carregar índice de cartões
        card_ids = set()
        for cm in comments:
            card_in_action = (cm.get("data", {}) or {}).get("card", {}) or {}
            if card_in_action.get("id"):
                card_ids.add(card_in_action["id"])
        cards_index = preload_cards_for_board(board_id, only_ids=card_ids)

        # 4) Processar comentários
        for idx, comment in enumerate(comments, 1):
            if idx % 200 == 0:
                logging.info(f"Processados {idx}/{total} comentários...")

            comment_id = str(comment.get("id", ""))
            comment_text = comment.get("data", {}).get("text", "")

            date_raw = comment.get("date")
            try:
                comment_date = (
                    datetime.fromisoformat(date_raw.replace("Z", "+00:00")).strftime("%Y-%m-%d")
                    if date_raw else None
                )
            except Exception:
                comment_date = None

            autor = comment.get("memberCreator", {}).get("fullName", "")

            # Dados do cartão a partir do action
            card = comment.get("data", {}).get("card", {}) or {}
            card_id = card.get("id")
            if not card_id:
                continue

            # Metadados do cartão no índice
            card_meta = cards_index.get(card_id)
            if not card_meta:
                # Raro (cartão movido de board ou permissão) — sem info, ignora
                logging.debug(f"Card {card_id} não está no índice; comentário ignorado.")
                continue

            # 4.1) Se o CARTÃO estiver arquivado, pula
            if bool(card_meta.get("closed")):
                continue

            lista_id_atual = card_meta.get("idList")
            if not lista_id_atual:
                continue

            # Info da lista atual (já no cache, ou fallback)
            lista_info = get_list_info(lista_id_atual)
            if not lista_info:
                continue

            # 4.2) Se a LISTA estiver arquivada, pula
            if bool(lista_info.get("closed")):
                continue

            # Dados finais
            lista_id = lista_info.get("id", "Sem ID")
            lista_name = lista_info.get("name", "Sem Lista")
            card_name = card_meta.get("name") or card.get("name", "Sem Nome")
            card_url = f"https://trello.com/c/{(card_meta.get('shortLink') or card.get('shortLink',''))}"

            # Tipo de campanha
            tipo = regras.get("default")
            tipo = regras.get("listas", {}).get(lista_id, tipo)

            # Extrair contatos do comentário
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
                        "nome_no_comentario": entry.get("nome"),
                        "email_no_comentario": (entry.get("email") or None),
                        "telefone": entry.get("telefone") or None,
                        "whatsapp": entry.get("whatsapp") or None,
                        "is_socio": entry.get("is_socio"),
                        "data": comment_date,
                        "id_board": board_id,
                        "tipo_de_campanha": tipo,
                    }
                )

    # --- DEDUPE GLOBAL ---
    if new_rows:
        before_count = len(new_rows)
        new_rows = dedupe_for_upsert(new_rows)
        logging.info(
            f"Registros após dedupe global: {len(new_rows)} (removidos {before_count - len(new_rows)})"
        )

    # --- UPSERT EM BATCH ---
    if new_rows:
        try:
            BATCH_SIZE = 500
            # reusar o mesmo client!
            supabase_client: Client = create_client(
                supabase_credentials["url"], supabase_credentials["key"]
            )
            for i in range(0, len(new_rows), BATCH_SIZE):
                batch = new_rows[i:i + BATCH_SIZE]
                batch_before = len(batch)
                batch = dedupe_for_upsert(batch)
                if len(batch) != batch_before:
                    logging.info(f"Dedupe no batch {i//BATCH_SIZE + 1}: {batch_before} -> {len(batch)}")
                supabase_client.from_(SUPABASE_TABLE_NAME) \
                    .upsert(batch, on_conflict="id_comentario,email_no_comentario,telefone") \
                    .execute()
            logging.info(f"Inseridos/atualizados {len(new_rows)} registros (após dedupe).")
        except Exception as e:
            logging.error(f"Erro no insert batch: {e}")
    else:
        logging.info("Nenhum novo registro encontrado após dedupe.")

if __name__ == "__main__":
    sync_trello_to_supabase()
