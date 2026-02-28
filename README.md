# NF Reader Bot

![NF Reader Bot Logo](profile_banenr/Gemini_Generated_Image_gtsx73gtsx73gtsx-removebg-preview.png)

O NF Reader é um bot do Telegram desenvolvido para escanear, processar e organizar Notas Fiscais Eletrônicas para Consumidor Final (NFC-e) usando as URLs dos QR Codes. Ele armazena os itens escaneados, extrai as listas de produtos e oferece resumos detalhados em infográficos e relatórios para ajudar você a acompanhar seus gastos.

## Funcionalidades

- Processamento de NFC-e a partir dos portais das Secretarias da Fazenda Estaduais (SEFAZ).
- Armazenamento local das informações das notas e produtos em formato CSV dentro de um diretório de banco de dados dedicado.
- Geração de infográficos em PNG e resumos em PDF.
- Permissões baseadas em cargos (Administrador, Moderador, Usuário).
- Limites de taxa para evitar spam (Envios e Relatórios).
- Categorização automática dos arquivos gerados em pastas específicas baseadas em suas extensões para facilitar a organização.

## Começando Rápido

1. Instale as dependências:
   ```bash
   pip install -r dependences.txt
   ```
2. Configure suas variáveis de ambiente copiando o arquivo `.env.example` para `.env`. Preencha o seu `BOT_TOKEN` e o seu `ADMIN-AUTH-TOKEN` do Telegram.
3. Inicie o bot:
   ```bash
   python bot.py
   ```

## Documentação

Para instruções mais detalhadas sobre configuração e uso, por favor, consulte o nosso [Guia de Uso](USAGE.md).
