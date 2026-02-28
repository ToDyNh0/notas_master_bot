# Guia de Uso

Este guia explica como configurar e usar corretamente o Bot do Telegram NF Reader.

## 1. Configuração do Ambiente

O bot utiliza um arquivo de ambiente para gerenciar informações confidenciais (secrets) sem colocá-las diretamente no código-fonte.
1. Copie o arquivo `.env.example` para criar um arquivo `.env`.
2. Insira o token do seu bot (fornecido pelo BotFather) na variável `BOT_TOKEN`.
3. Insira o seu ID de Usuário do Telegram na variável `ADMIN-AUTH-TOKEN`. Este usuário receberá permissões totais de administrador.

## 2. Dependências

Para instalar as dependências do projeto, use o seguinte comando no bash:
```bash
pip install -r dependences.txt
```

## 3. Iniciando o Bot

Execute o script do bot:
```bash
python bot.py
```
Ao ser iniciado, o bot irá gerar automaticamente todos os diretórios de saída necessários (por exemplo, `output/pdf`, `output/json`, `output/db`) e começará a ouvir as mensagens recebidas.

## 4. Cargos de Usuários e Comandos

O aplicativo define três cargos básicos: Administrador, Moderador e Usuário.

**Comandos de Administrador:**
- `/addrole`: Conceder cargos específicos para outros usuários.
- `/removerole`: Revogar cargos de usuários.
- `/logs`: Ver os registros (logs) de atividades recentes do bot.
- `/users`: Ver a atividade por usuário.
- Todos os comandos de Moderador.

**Comandos de Moderador:**
- `/resume [dia|semana|quinzena|mes]`: Solicitar relatórios visuais. (Limitado a 1 relatório por tipo por hora).
- `/nf`: Enviar links de NF ou códigos QR para processamento. (Limitado a 1 nota a cada 50 segundos).
- Todos os comandos de Usuário.

**Comandos de Usuário:**
- `/myid`: Descobrir o seu ID exato do Telegram para solicitar permissão de administrador.
- `/help`: Ver instruções básicas de ajuda.

## 5. Enviando Notas Fiscais

Assim que for autorizado como Moderador ou Administrador, você pode simplesmente encaminhar a URL da NFC-e ou enviar uma foto do QR code para o bot (use a legenda `/nf`). O bot extrairá os dados da NF do portal apropriado da SEFAZ, responderá com um recibo em infográfico e salvará os produtos de forma segura no banco de dados local em formato CSV.
