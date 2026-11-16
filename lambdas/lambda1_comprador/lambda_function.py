"""
Serviço do Comprador (sebo-comprador-service)

Gerencia as operações voltadas ao cliente final do sebo:
- Listagem do catálogo de livros;
- Solicitação de compra (reserva imediata caso haja estoque ou inclusão na fila de espera);
- Consulta de pedidos realizados pelo comprador.
"""

import json
import os
import re
import unicodedata
import uuid
from datetime import datetime, timezone
from decimal import Decimal

import boto3
from boto3.dynamodb.conditions import Attr
from botocore.exceptions import ClientError

# Configuração do ambiente e clientes AWS
REGIAO_AWS = os.environ.get("AWS_REGION", "us-east-1")
NOME_TABELA_LIVROS = os.environ.get("TABLE_LIVROS", "SeboLivros")
NOME_TABELA_PEDIDOS = os.environ.get("TABLE_PEDIDOS", "SeboPedidos")
NOME_TOPICO_SNS = os.environ.get("SNS_TOPIC_NAME", "SeboNotificacoes")

dynamodb = boto3.resource("dynamodb", region_name=REGIAO_AWS)
sns_client = boto3.client("sns", region_name=REGIAO_AWS)

tabela_livros = dynamodb.Table(NOME_TABELA_LIVROS)
tabela_pedidos = dynamodb.Table(NOME_TABELA_PEDIDOS)

CORS_HEADERS = {
    "Content-Type": "application/json; charset=utf-8",
    "Access-Control-Allow-Origin": "*",
    "Access-Control-Allow-Methods": "OPTIONS,POST,GET",
    "Access-Control-Allow-Headers": "Content-Type,Authorization",
}


class DecimalEncoder(json.JSONEncoder):
    """Converte valores Decimal retornados pelo DynamoDB em int ou float para serialização JSON."""
    def default(self, obj):
        if isinstance(obj, Decimal):
            return int(obj) if obj % 1 == 0 else float(obj)
        return super().default(obj)


def montar_resposta(status_code: int, dados: dict) -> dict:
    """Formata a resposta no padrão HTTP esperado pela Function URL da Lambda."""
    return {
        "statusCode": status_code,
        "headers": CORS_HEADERS,
        "body": json.dumps(dados, ensure_ascii=False, cls=DecimalEncoder),
    }


def extrair_metodo_http(event: dict) -> str:
    """Retorna o método HTTP da requisição (GET, POST, OPTIONS)."""
    return (
        event.get("requestContext", {}).get("http", {}).get("method")
        or event.get("httpMethod")
        or "POST"
    ).upper()


def extrair_dados_evento(event: dict) -> dict:
    """Extrai o payload JSON tanto de requisições via Function URL quanto de testes diretos no console."""
    if "body" in event and event["body"] is not None:
        if isinstance(event["body"], str) and event["body"].strip():
            return json.loads(event["body"])
        if isinstance(event["body"], dict):
            return event["body"]

    parametros_url = event.get("queryStringParameters") or {}
    if parametros_url:
        return parametros_url

    return event if isinstance(event, dict) else {}


def normalizar_livro_id(texto: str) -> str:
    """Gera um identificador padronizado a partir do título (ex: 'Dom Casmurro' -> 'dom-casmurro')."""
    sem_acentos = unicodedata.normalize("NFKD", texto).encode("ascii", "ignore").decode("ascii")
    slug = re.sub(r"[^a-z0-9]+", "-", sem_acentos.lower()).strip("-")
    return slug or "livro-sem-id"


def agora_iso() -> str:
    """Retorna o timestamp atual em UTC no formato ISO 8601."""
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def agora_iso_milissegundos() -> str:
    """
    Retorna o timestamp atual em UTC no formato ISO 8601 com milissegundos (ex: 2026-11-08T19:45:00.123Z).
    Usado em 'criado_em' dos pedidos, que define a ordem da fila de espera.
    """
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.%f")[:-3] + "Z"


def escanear_tabela_completa(tabela, **parametros_scan) -> list:
    """Lê todos os itens de uma tabela DynamoDB percorrendo todas as páginas (LastEvaluatedKey)."""
    itens = []
    parametros = dict(parametros_scan)
    while True:
        resposta = tabela.scan(**parametros)
        itens.extend(resposta.get("Items", []))
        chave_continuacao = resposta.get("LastEvaluatedKey")
        if not chave_continuacao:
            break
        parametros["ExclusiveStartKey"] = chave_continuacao
    return itens


def obter_arn_topico_sns() -> str:
    """Obtém o ARN do tópico SNS via variável de ambiente ou buscando pelo nome na conta."""
    arn_ambiente = os.environ.get("SNS_TOPIC_ARN", "").strip()
    if arn_ambiente:
        return arn_ambiente

    paginador = sns_client.get_paginator("list_topics")
    for pagina in paginador.paginate():
        for topico in pagina.get("Topics", []):
            arn = topico["TopicArn"]
            if arn.endswith(f":{NOME_TOPICO_SNS}"):
                return arn

    raise RuntimeError(f"Tópico SNS '{NOME_TOPICO_SNS}' não encontrado na conta AWS.")


def enviar_notificacao_email(assunto: str, mensagem: str, comprador_email: str) -> bool:
    """
    Publica um aviso por e-mail no tópico do Amazon SNS.
    Inclui 'comprador_email' em MessageAttributes para suportar Filter Policy por assinante.
    """
    try:
        topic_arn = obter_arn_topico_sns()
        sns_client.publish(
            TopicArn=topic_arn,
            Subject=assunto[:100],
            Message=mensagem,
            MessageAttributes={
                "comprador_email": {
                    "DataType": "String",
                    "StringValue": comprador_email or "desconhecido",
                },
            },
        )
        return True
    except Exception as erro:
        print(f"[AVISO SNS] Falha ao publicar no SNS: {erro}")
        return False


def listar_livros() -> dict:
    """Busca e retorna todos os livros registrados na tabela SeboLivros."""
    livros = escanear_tabela_completa(tabela_livros)
    livros.sort(key=lambda item: item.get("titulo", ""))

    return montar_resposta(200, {
        "sucesso": True,
        "total": len(livros),
        "livros": livros,
    })


def tentar_reservar_estoque_atomico(livro_id: str) -> dict | None:
    """
    Decrementa 1 unidade do estoque diretamente no DynamoDB apenas se quantidade > 0.
    Retorna os atributos atualizados do livro se conseguir reservar, ou None caso não haja estoque.
    """
    try:
        resposta = tabela_livros.update_item(
            Key={"livro_id": livro_id},
            UpdateExpression=(
                "SET quantidade = quantidade - :um, "
                "total_vendidos = if_not_exists(total_vendidos, :zero) + :um, "
                "atualizado_em = :agora"
            ),
            ConditionExpression="attribute_exists(livro_id) AND quantidade > :zero",
            ExpressionAttributeValues={
                ":um": 1,
                ":zero": 0,
                ":agora": agora_iso(),
            },
            ReturnValues="ALL_NEW",
        )
        return resposta.get("Attributes")
    except ClientError as erro:
        codigo_erro = erro.response["Error"]["Code"]
        if codigo_erro == "ConditionalCheckFailedException":
            return None
        raise


def devolver_exemplar_ao_estoque(livro_id: str) -> None:
    """Devolve 1 unidade ao estoque caso ocorra falha ao persistir o pedido ou corrida com o Stream."""
    tabela_livros.update_item(
        Key={"livro_id": livro_id},
        UpdateExpression=(
            "SET quantidade = quantidade + :um, "
            "total_vendidos = total_vendidos - :um, "
            "atualizado_em = :agora"
        ),
        ExpressionAttributeValues={
            ":um": 1,
            ":agora": agora_iso(),
        },
    )


def garantir_livro_no_catalogo(livro_id: str, titulo: str) -> dict:
    """Garante que o livro solicitado exista na tabela SeboLivros (criando com estoque 0 se necessário)."""
    consulta = tabela_livros.get_item(Key={"livro_id": livro_id})
    if "Item" in consulta:
        return consulta["Item"]

    novo_livro = {
        "livro_id": livro_id,
        "titulo": titulo,
        "autor": "A cadastrar pelo Sebo",
        "preco": "0.00",
        "quantidade": 0,
        "total_vendidos": 0,
        "atualizado_em": agora_iso(),
    }
    try:
        tabela_livros.put_item(
            Item=novo_livro,
            ConditionExpression="attribute_not_exists(livro_id)",
        )
        return novo_livro
    except ClientError:
        return tabela_livros.get_item(Key={"livro_id": livro_id}).get("Item", novo_livro)


def montar_email_disponivel(comprador_nome: str, comprador_email: str, titulo: str, livro_id: str, pedido_id: str, momento: str) -> tuple:
    """Monta o assunto e o corpo do e-mail para exemplar disponível em estoque."""
    assunto = f"[Sebo AWS] Livro disponivel para retirada: {titulo}"
    corpo = (
        f"Olá, {comprador_nome}!\n\n"
        f"Boa notícia: o livro '{titulo}' está disponível em nosso estoque "
        f"e um exemplar já foi separado para você (Pedido {pedido_id}).\n\n"
        f"Por favor, compareça à livraria para concretizar a compra e retirar o seu livro.\n\n"
        f"Dados do pedido:\n"
        f"- Código: {pedido_id}\n"
        f"- Livro: {titulo} ({livro_id})\n"
        f"- Comprador: {comprador_nome} ({comprador_email})\n"
        f"- Data/Hora: {momento}\n"
    )
    return assunto, corpo


def promover_pedido_para_disponivel(pedido_id: str, momento: str) -> bool:
    """
    Atualiza o pedido recém-criado de AGUARDANDO_ESTOQUE para DISPONIVEL_NOTIFICADO
    apenas se ele ainda não tiver sido atendido concorrentemente pela Lambda 3.
    """
    try:
        tabela_pedidos.update_item(
            Key={"pedido_id": pedido_id},
            UpdateExpression="SET #status = :novo, notificado_em = :momento, observacao = :obs",
            ConditionExpression="#status = :aguardando",
            ExpressionAttributeNames={"#status": "status"},
            ExpressionAttributeValues={
                ":novo": "DISPONIVEL_NOTIFICADO",
                ":aguardando": "AGUARDANDO_ESTOQUE",
                ":momento": momento,
                ":obs": "Livro em estoque no ato do pedido. Cliente avisado para comparecer ao sebo.",
            },
        )
        return True
    except ClientError as erro:
        if erro.response["Error"]["Code"] == "ConditionalCheckFailedException":
            return False
        raise


def existe_pedido_anterior_na_fila(livro_id: str, referencia: tuple | None = None) -> bool:
    """
    Indica se há pedidos AGUARDANDO_ESTOQUE para o livro que devem ser atendidos antes.
    Sem 'referencia', considera qualquer pedido na fila. Com 'referencia' = (criado_em, pedido_id),
    considera apenas os pedidos mais antigos que ela (mesma ordem FIFO usada pelo notificador).
    """
    filtro = Attr("livro_id").eq(livro_id) & Attr("status").eq("AGUARDANDO_ESTOQUE")
    fila = escanear_tabela_completa(tabela_pedidos, FilterExpression=filtro, ConsistentRead=True)

    for pedido in fila:
        if pedido.get("livro_id") != livro_id or pedido.get("status") != "AGUARDANDO_ESTOQUE":
            continue
        if referencia is None:
            return True
        if (pedido.get("criado_em", ""), pedido.get("pedido_id", "")) < referencia:
            return True
    return False


def solicitar_livro(dados: dict) -> dict:
    """
    Registra o pedido de compra de um livro.
    Se houver estoque disponível e ninguém aguardando na fila, grava o pedido, reserva o exemplar
    e envia confirmação via SNS. Caso contrário, salva o pedido como AGUARDANDO_ESTOQUE na fila de espera.
    """
    titulo_informado = (dados.get("titulo") or "").strip()
    livro_id = (dados.get("livro_id") or "").strip() or normalizar_livro_id(titulo_informado)
    comprador_nome = (dados.get("comprador_nome") or "").strip()
    comprador_email = (dados.get("comprador_email") or "").strip()

    if not livro_id or not comprador_nome or not comprador_email:
        return montar_resposta(400, {
            "sucesso": False,
            "mensagem": "Informe 'titulo' (ou 'livro_id'), 'comprador_nome' e 'comprador_email'.",
        })

    pedido_id = f"PED-{uuid.uuid4().hex[:8].upper()}"
    momento_atual = agora_iso_milissegundos()

    # Se já há clientes na fila, o exemplar disponível pertence a eles: o novo pedido entra no fim da fila
    if existe_pedido_anterior_na_fila(livro_id):
        livro_reservado = None
    else:
        livro_reservado = tentar_reservar_estoque_atomico(livro_id)

    # Caso 1: Livro disponível em estoque -> persiste o pedido primeiro e depois envia o e-mail
    if livro_reservado is not None:
        titulo_oficial = livro_reservado.get("titulo", titulo_informado or livro_id)

        registro_pedido = {
            "pedido_id": pedido_id,
            "livro_id": livro_id,
            "titulo": titulo_oficial,
            "comprador_nome": comprador_nome,
            "comprador_email": comprador_email,
            "status": "DISPONIVEL_NOTIFICADO",
            "criado_em": momento_atual,
            "notificado_em": momento_atual,
            "observacao": "Livro em estoque no ato do pedido. Cliente avisado para comparecer ao sebo.",
        }

        try:
            tabela_pedidos.put_item(Item=registro_pedido)
        except Exception:
            devolver_exemplar_ao_estoque(livro_id)
            raise

        assunto, corpo_email = montar_email_disponivel(
            comprador_nome, comprador_email, titulo_oficial, livro_id, pedido_id, momento_atual
        )
        email_enviado = enviar_notificacao_email(assunto, corpo_email, comprador_email)

        if not email_enviado:
            registro_pedido["notificado_em"] = None
            registro_pedido["observacao"] = "Exemplar separado, mas houve falha no envio do e-mail via SNS."
            tabela_pedidos.update_item(
                Key={"pedido_id": pedido_id},
                UpdateExpression="SET notificado_em = :nulo, observacao = :obs",
                ExpressionAttributeValues={
                    ":nulo": None,
                    ":obs": registro_pedido["observacao"],
                },
            )

        return montar_resposta(200, {
            "sucesso": True,
            "disponivel_imediato": True,
            "email_enviado": email_enviado,
            "mensagem": (
                f"O livro '{titulo_oficial}' está disponível! "
                f"Separamos seu exemplar e enviamos um e-mail para {comprador_email} "
                f"para você comparecer à livraria e concretizar a compra."
            ),
            "pedido": registro_pedido,
            "estoque_restante": livro_reservado.get("quantidade", 0),
        })

    # Caso 2: Livro sem estoque -> registra encomenda na fila de espera
    livro_catalogo = garantir_livro_no_catalogo(livro_id, titulo_informado or livro_id)
    titulo_oficial = livro_catalogo.get("titulo", titulo_informado or livro_id)

    registro_pedido = {
        "pedido_id": pedido_id,
        "livro_id": livro_id,
        "titulo": titulo_oficial,
        "comprador_nome": comprador_nome,
        "comprador_email": comprador_email,
        "status": "AGUARDANDO_ESTOQUE",
        "criado_em": momento_atual,
        "notificado_em": None,
        "observacao": "Livro sem estoque no momento. Interesse salvo na fila de espera.",
    }
    tabela_pedidos.put_item(Item=registro_pedido)

    # Nova checagem atômica: cobre o caso em que o estoque foi reposto exatamente
    # entre a primeira tentativa e a gravação do pedido na fila.
    # Só tenta se este pedido for o primeiro da fila, para não passar à frente de quem chegou antes.
    livro_reposto = None
    if not existe_pedido_anterior_na_fila(livro_id, (momento_atual, pedido_id)):
        livro_reposto = tentar_reservar_estoque_atomico(livro_id)
    if livro_reposto is not None:
        if promover_pedido_para_disponivel(pedido_id, momento_atual):
            assunto, corpo_email = montar_email_disponivel(
                comprador_nome, comprador_email, titulo_oficial, livro_id, pedido_id, momento_atual
            )
            email_enviado = enviar_notificacao_email(assunto, corpo_email, comprador_email)
            registro_pedido["status"] = "DISPONIVEL_NOTIFICADO"
            registro_pedido["notificado_em"] = momento_atual if email_enviado else None
            registro_pedido["observacao"] = "Livro em estoque no ato do pedido. Cliente avisado para comparecer ao sebo."
            return montar_resposta(200, {
                "sucesso": True,
                "disponivel_imediato": True,
                "email_enviado": email_enviado,
                "mensagem": (
                    f"O livro '{titulo_oficial}' está disponível! "
                    f"Separamos seu exemplar e enviamos um e-mail para {comprador_email} "
                    f"para você comparecer à livraria e concretizar a compra."
                ),
                "pedido": registro_pedido,
                "estoque_restante": livro_reposto.get("quantidade", 0),
            })
        else:
            # A Lambda 3 já havia atendido este pedido via Stream; devolve o exemplar extra
            devolver_exemplar_ao_estoque(livro_id)

    return montar_resposta(200, {
        "sucesso": True,
        "disponivel_imediato": False,
        "email_enviado": False,
        "mensagem": (
            f"O livro '{titulo_oficial}' não está disponível em estoque no momento. "
            f"Seu interesse foi registrado na fila de espera (Pedido {pedido_id}) e "
            f"você receberá um e-mail automaticamente assim que um exemplar chegar ao sebo!"
        ),
        "pedido": registro_pedido,
    })


def consultar_meus_pedidos(dados: dict) -> dict:
    """Retorna os pedidos registrados, permitindo filtrar pelo e-mail do comprador."""
    email_filtro = (dados.get("comprador_email") or "").strip().lower()
    pedidos = escanear_tabela_completa(tabela_pedidos)

    if email_filtro:
        pedidos = [
            p for p in pedidos
            if (p.get("comprador_email") or "").strip().lower() == email_filtro
        ]

    pedidos.sort(key=lambda item: item.get("criado_em", ""), reverse=True)

    return montar_resposta(200, {
        "sucesso": True,
        "total": len(pedidos),
        "pedidos": pedidos,
    })


def lambda_handler(event, context):
    """Ponto de entrada da função Lambda: roteia a requisição de acordo com o campo 'acao'."""
    metodo = extrair_metodo_http(event)

    if metodo == "OPTIONS":
        return montar_resposta(200, {"ok": True})

    try:
        dados = extrair_dados_evento(event)
        acao = (dados.get("acao") or "").strip().lower()

        if metodo == "GET" and not acao:
            acao = "listar_livros"

        if acao == "listar_livros":
            return listar_livros()
        elif acao == "solicitar_livro":
            return solicitar_livro(dados)
        elif acao == "meus_pedidos":
            return consultar_meus_pedidos(dados)
        else:
            return montar_resposta(400, {
                "sucesso": False,
                "mensagem": f"Ação '{acao}' inválida. Use: 'listar_livros', 'solicitar_livro' ou 'meus_pedidos'.",
            })

    except Exception as erro:
        print(f"[ERRO LAMBDA COMPRADOR] {erro}")
        return montar_resposta(500, {
            "sucesso": False,
            "mensagem": f"Erro interno na Lambda do Comprador: {str(erro)}",
        })
