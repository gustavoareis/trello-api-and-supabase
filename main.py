import pandas as pd
from trello import TrelloClient
import re
from unidecode import unidecode
from credentials import trello_credentials, supabase_credentials
from supabase import create_client, Client

LISTA_ALVO = "CADÊNCIA DE NUTRIÇÃO"
MEMBRO_ALVO = "samantha"
client = TrelloClient(
    api_key=trello_credentials['api_key'],
    token=trello_credentials['token']
)

supabase: Client = create_client(
    supabase_credentials['url'],
    supabase_credentials['key']
)
supabase_table_name = "trello_comentarios"

def extract_info(text):
    if not text:
        return [], []

    name_pattern = r'"[A-ZÀ-Ü\s]+"|\b[A-ZÀ-Ü][A-ZÀ-Ü\s]+\b'
    email_pattern = r'[a-zA-Z0-9.%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}'

    names = re.findall(name_pattern, text)
    emails = re.findall(email_pattern, text)

    cleaned_names = [name.strip('"').strip() for name in names if len(name.split()) >= 2]
    cleaned_emails = [email.lower() for email in emails]

    return cleaned_names, cleaned_emails

board = client.get_board('e30OHAsU')
print("🔎 Buscando comentários no Trello e sincronizando com o Supabase...")

try:
    for lista in board.all_lists():
        if unidecode(lista.name.lower()) == unidecode(LISTA_ALVO.lower()):
            for card in lista.list_cards():
                for comment in card.fetch_comments():
                    autor = unidecode(comment['memberCreator'].get('fullName', '').lower())
                    texto_comentario = comment['data']['text']

                    if MEMBRO_ALVO in autor or MEMBRO_ALVO in unidecode(texto_comentario.lower()):
                        nomes, emails = extract_info(texto_comentario)
                        nome = nomes[0] if nomes else ""
                        email = emails[0] if emails else ""

                        supabase_data = {
                            "id_comentario": str(comment['id']),
                            "id_cartao": str(card.id),
                            "lista": str(lista.name),
                            "cartao": str(card.name),
                            "url": str(card.url),
                            "autor_comentario": str(comment['memberCreator']['fullName']),
                            "nome_no_comentario": str(nome),
                            "email_no_comentario": str(email),
                            "data": pd.to_datetime(comment['date']).strftime('%Y-%m-%d')
                        }

                        response = supabase.from_(supabase_table_name).upsert(supabase_data).execute()

                        if response.data:
                            print(f"Registro para o comentário '{comment['id']}' enviado com sucesso.")
                        else:
                            print(f"\nErro ao enviar registro (ID: {comment['id']}): {response.error}")

except Exception as e:
    print(f"\nOcorreu um erro geral durante a execução: {e}")

print("\nSincronização concluída!")
