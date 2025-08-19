Trello → Supabase Sync

Este projeto tem como objetivo extrair comentários do Trello de uma lista específica, filtrar informações relevantes (como nomes e e-mails) e sincronizar esses dados com uma tabela no Supabase.

🔧 Funcionalidades

  -Conecta-se à API do Trello usando trello-python.

  -Filtra comentários feitos em uma lista e por um membro específico.

  -Extrai automaticamente: Nome citado no comentário e E-mail citado no comentário

  -Sincroniza os dados com uma tabela no Supabase (trello_comentarios).

  -Evita duplicados através do uso de upsert.
