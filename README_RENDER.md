# Orçamento Fortlev no Render

## Rodando local
```bash
pip install -r requirements.txt
python app.py
```

## Publicar no Render
- Suba esta pasta para um repositório GitHub.
- No Render, crie um **Web Service** a partir do repositório.
- O `render.yaml` já configura build e start automaticamente.
- Para manter o SQLite sem perder dados após deploy/restart, use um **Persistent Disk** (plano pago) e ajuste `DATA_DIR` para o caminho do disco, por exemplo `/var/data`.

## Login padrão
- usuário: `admin`
- senha: `admin`

## Observação importante
Sem Persistent Disk, o SQLite funciona, mas as alterações podem ser perdidas em novos deploys ou reinícios do serviço.
