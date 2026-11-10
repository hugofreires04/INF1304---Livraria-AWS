"""
Serviço Notificador de Estoque (sebo-notificador-estoque)

Worker reativo acionado pelo DynamoDB Stream da tabela SeboLivros:
- Detecta inserções/modificações de livros com estoque disponível (quantidade > 0);
- Atende a fila de espera daquele livro em ordem de chegada (FIFO), reservando
  um exemplar por pedido de forma atômica;
- Marca cada pedido atendido como CHEGOU_NOTIFICADO e avisa o comprador por e-mail via SNS.

Também aceita invocação direta para testes no console, ex: {"livro_id": "dom-casmurro"}.
"""

import os
import re
import unicodedata
from datetime import datetime, timezone

import boto3
from boto3.dynamodb.conditions import Attr
from boto3.dynamodb.types import TypeDeserializer
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

desserializador = TypeDeserializer()

# Cache do ARN entre invocações do mesmo container (evita chamar list_topics a cada evento)
_arn_topico_cache = None


def agora_iso() -> str:
    """Retorna o timestamp atual em UTC no formato ISO 8601."""
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def normalizar_livro_id(texto: str) -> str:
    """Gera um identificador padronizado a partir do título (ex: 'Dom Casmurro' -> 'dom-casmurro')."""
    sem_acentos = unicodedata.normalize("NFKD", texto).encode("ascii", "ignore").decode("ascii")
    slug = re.sub(r"[^a-z0-9]+", "-", sem_acentos.lower()).strip("-")
    return slug or "livro-sem-id"


def desserializar_imagem(imagem_tipada: dict) -> dict:
    """
    Converte uma imagem do DynamoDB Stream do formato tipado
    (ex: {"titulo": {"S": "Dom Casmurro"}, "quantidade": {"N": "2"}})
    para um dicionário Python comum (ex: {"titulo": "Dom Casmurro", "quantidade": Decimal("2")}).
    """
    return {chave: desserializador.deserialize(valor) for chave, valor in imagem_tipada.items()}


def obter_arn_topico_sns() -> str:
    """Obtém o ARN do tópico SNS via variável de ambiente ou buscando pelo nome na conta."""
    global _arn_topico_cache
    if _arn_topico_cache:
        return _arn_topico_cache

    arn_ambiente = os.environ.get("SNS_TOPIC_ARN", "").strip()
    if arn_ambiente:
        _arn_topico_cache = arn_ambiente
        return arn_ambiente

    paginador = sns_client.get_paginator("list_topics")
    for pagina in paginador.paginate():
        for topico in pagina.get("Topics", []):
            arn = topico["TopicArn"]
            if arn.endswith(f":{NOME_TOPICO_SNS}"):
                _arn_topico_cache = arn
                return arn

    raise RuntimeError(f"Tópico SNS '{NOME_TOPICO_SNS}' não encontrado na conta AWS.")


def enviar_notificacao_email(assunto: str, mensagem: str, comprador_email: str) -> bool:
    """
    Publica um aviso por e-mail no tópico do Amazon SNS.
    O e-mail do comprador vai como atributo da mensagem, permitindo usar
    filtros de assinatura (filter policy) no SNS, se desejado.
    """
    try:
        sns_client.publish(
            TopicArn=obter_arn_topico_sns(),
            Subject=assunto[:100],
            Message=mensagem,
            MessageAttributes={
                "comprador_email": {"DataType": "String", "StringValue": comprador_email or "desconhecido"},
            },
        )
        return True
    except Exception as erro:
        print(f"[AVISO SNS] Falha ao publicar no SNS: {erro}")
        return False


def extrair_livros_do_evento(event: dict) -> list:
    """
    Monta a lista de livros a processar a partir do evento recebido.
    - Evento do DynamoDB Stream: usa o NewImage de cada registro INSERT/MODIFY.
    - Invocação direta de teste: busca o livro informado em 'livro_id' (ou 'titulo') na tabela.
    """
    registros = event.get("Records")

    if registros is None:
        livro_id = (event.get("livro_id") or "").strip()
        if not livro_id and event.get("titulo"):
            livro_id = normalizar_livro_id(event["titulo"])
        if not livro_id:
            print("[NOTIFICADOR] Evento sem 'Records' e sem 'livro_id'. Nada a processar.")
            return []

        item = tabela_livros.get_item(Key={"livro_id": livro_id}, ConsistentRead=True).get("Item")
        if not item:
            print(f"[NOTIFICADOR] Livro '{livro_id}' não encontrado na tabela {NOME_TABELA_LIVROS}.")
            return []
        return [item]

    livros = []
    for registro in registros:
        nome_evento = registro.get("eventName")
        if nome_evento not in ("INSERT", "MODIFY"):
            continue

        imagem_nova = registro.get("dynamodb", {}).get("NewImage")
        if not imagem_nova:
            continue

        livros.append(desserializar_imagem(imagem_nova))
    return livros


def buscar_fila_de_espera(livro_id: str) -> list:
    """
    Retorna os pedidos AGUARDANDO_ESTOQUE de um livro, ordenados por 'criado_em'
    crescente (FIFO: quem entrou primeiro na fila é atendido primeiro).
    """
    filtro = Attr("livro_id").eq(livro_id) & Attr("status").eq("AGUARDANDO_ESTOQUE")
    parametros = {"FilterExpression": filtro, "ConsistentRead": True}

    pedidos = []
    while True:
        resposta = tabela_pedidos.scan(**parametros)
        pedidos.extend(resposta.get("Items", []))
        chave_continuacao = resposta.get("LastEvaluatedKey")
        if not chave_continuacao:
            break
        parametros["ExclusiveStartKey"] = chave_continuacao

    pedidos.sort(key=lambda p: p.get("criado_em", ""))
    return pedidos


def reservar_exemplar_atomico(livro_id: str) -> bool:
    """
    Decrementa 1 unidade de 'quantidade' e soma 1 em 'total_vendidos' somente se houver estoque.
    A condição é verificada pelo próprio DynamoDB, evitando condição de corrida com a Lambda do Comprador.
    Retorna False se o estoque acabou no momento da operação.
    """
    try:
        tabela_livros.update_item(
            Key={"livro_id": livro_id},
            UpdateExpression=(
                "SET quantidade = quantidade - :um, "
                "total_vendidos = if_not_exists(total_vendidos, :zero) + :um, "
                "atualizado_em = :agora"
            ),
            ConditionExpression="attribute_exists(livro_id) AND quantidade > :zero",
            ExpressionAttributeValues={":um": 1, ":zero": 0, ":agora": agora_iso()},
        )
        return True
    except ClientError as erro:
        if erro.response["Error"]["Code"] == "ConditionalCheckFailedException":
            return False
        raise


def devolver_exemplar(livro_id: str) -> None:
    """Desfaz uma reserva (devolve 1 unidade ao estoque) quando o pedido já foi atendido por outra execução."""
    tabela_livros.update_item(
        Key={"livro_id": livro_id},
        UpdateExpression=(
            "SET quantidade = quantidade + :um, "
            "total_vendidos = total_vendidos - :um, "
            "atualizado_em = :agora"
        ),
        ExpressionAttributeValues={":um": 1, ":agora": agora_iso()},
    )


def marcar_pedido_como_notificado(pedido_id: str, momento: str) -> bool:
    """
    Muda o status do pedido de AGUARDANDO_ESTOQUE para CHEGOU_NOTIFICADO.
    A condição garante que o mesmo pedido não seja atendido duas vezes por execuções concorrentes.
    Retorna False se o pedido já não estava mais aguardando.
    """
    try:
        tabela_pedidos.update_item(
            Key={"pedido_id": pedido_id},
            UpdateExpression="SET #status = :novo, notificado_em = :momento, observacao = :obs",
            ConditionExpression="#status = :aguardando",
            ExpressionAttributeNames={"#status": "status"},
            ExpressionAttributeValues={
                ":novo": "CHEGOU_NOTIFICADO",
                ":aguardando": "AGUARDANDO_ESTOQUE",
                ":momento": momento,
                ":obs": "Livro chegou ao sebo. Exemplar separado e cliente avisado para comparecer à livraria.",
            },
        )
        return True
    except ClientError as erro:
        if erro.response["Error"]["Code"] == "ConditionalCheckFailedException":
            return False
        raise


def registrar_falha_email(pedido_id: str) -> None:
    """Registra no pedido que o exemplar foi separado, mas o e-mail de aviso não pôde ser enviado."""
    tabela_pedidos.update_item(
        Key={"pedido_id": pedido_id},
        UpdateExpression="SET notificado_em = :nulo, observacao = :obs",
        ExpressionAttributeValues={
            ":nulo": None,
            ":obs": "Livro chegou ao sebo e o exemplar foi separado, mas houve falha no envio do e-mail via SNS.",
        },
    )


def montar_email_chegada(pedido: dict, titulo: str) -> tuple:
    """Gera o assunto e o corpo do e-mail avisando o comprador sobre a chegada do livro."""
    nome = pedido.get("comprador_nome", "cliente")
    email = pedido.get("comprador_email", "")
    pedido_id = pedido.get("pedido_id", "")

    assunto = f"[Sebo AWS] Seu livro chegou: {titulo}"
    corpo = (
        f"Olá, {nome}!\n\n"
        f"O livro '{titulo}' que você encomendou acabou de chegar ao nosso sebo!\n"
        f"Já separamos um exemplar para você (Pedido {pedido_id}).\n\n"
        f"Por favor, compareça à livraria para concretizar a compra e retirar o seu livro.\n\n"
        f"Dados do pedido:\n"
        f"- Código: {pedido_id}\n"
        f"- Livro: {titulo} ({pedido.get('livro_id', '')})\n"
        f"- Comprador: {nome} ({email})\n"
        f"- Pedido feito em: {pedido.get('criado_em', '')}\n"
        f"- Aviso enviado em: {agora_iso()}\n"
    )
    return assunto, corpo


def atender_fila_do_livro(livro: dict) -> list:
    """
    Distribui os exemplares disponíveis de um livro entre os clientes da fila de espera (FIFO).
    Para cada pedido: reserva 1 exemplar, marca o pedido como CHEGOU_NOTIFICADO e envia o e-mail.
    Retorna a lista de pedidos atendidos nesta execução.
    """
    livro_id = livro.get("livro_id")
    quantidade = int(livro.get("quantidade", 0) or 0)

    if not livro_id or quantidade <= 0:
        return []

    fila = buscar_fila_de_espera(livro_id)
    if not fila:
        print(f"[NOTIFICADOR] '{livro_id}' tem {quantidade} exemplar(es), mas ninguém na fila de espera.")
        return []

    print(f"[NOTIFICADOR] '{livro_id}': {quantidade} exemplar(es) e {len(fila)} cliente(s) na fila.")
    titulo = livro.get("titulo") or livro_id
    atendidos = []

    for pedido in fila:
        pedido_id = pedido["pedido_id"]

        # 1. Reserva atômica de 1 exemplar; se falhar, o estoque acabou e a fila para aqui
        if not reservar_exemplar_atomico(livro_id):
            print(f"[NOTIFICADOR] Estoque de '{livro_id}' esgotado. Restante da fila continua aguardando.")
            break

        # 2. Marca o pedido como atendido; se outra execução já o atendeu, devolve o exemplar
        momento = agora_iso()
        if not marcar_pedido_como_notificado(pedido_id, momento):
            print(f"[NOTIFICADOR] Pedido {pedido_id} já havia sido atendido. Devolvendo exemplar ao estoque.")
            devolver_exemplar(livro_id)
            continue

        # 3. Avisa o comprador por e-mail via SNS
        assunto, corpo = montar_email_chegada(pedido, titulo)
        email_enviado = enviar_notificacao_email(assunto, corpo, pedido.get("comprador_email", ""))
        if not email_enviado:
            registrar_falha_email(pedido_id)

        print(f"[NOTIFICADOR] Pedido {pedido_id} atendido (e-mail enviado: {email_enviado}).")
        atendidos.append({
            "pedido_id": pedido_id,
            "livro_id": livro_id,
            "comprador_email": pedido.get("comprador_email"),
            "email_enviado": email_enviado,
        })

    return atendidos


def lambda_handler(event, context):
    """
    Ponto de entrada da função Lambda.
    Processa cada livro do evento de forma independente: um erro em um registro é
    registrado no log sem impedir o processamento dos demais.
    """
    livros = extrair_livros_do_evento(event if isinstance(event, dict) else {})
    pedidos_atendidos = []
    erros = []

    for livro in livros:
        try:
            pedidos_atendidos.extend(atender_fila_do_livro(livro))
        except Exception as erro:
            print(f"[ERRO LAMBDA NOTIFICADOR] Livro '{livro.get('livro_id')}': {erro}")
            erros.append({"livro_id": livro.get("livro_id"), "erro": str(erro)})

    resultado = {
        "sucesso": not erros,
        "livros_processados": len(livros),
        "total_atendidos": len(pedidos_atendidos),
        "pedidos_atendidos": pedidos_atendidos,
        "erros": erros,
    }
    print(f"[NOTIFICADOR] Resultado: {resultado}")
    return resultado
