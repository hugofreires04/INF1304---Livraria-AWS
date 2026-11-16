"""
Serviço do Colaborador (sebo-colaborador-service)

Gerencia as operações internas da livraria (sebo):
- Cadastro de novos livros e reposição de exemplares em estoque (preservando autor e preço existentes);
- Painel consolidado com catálogo, histórico de vendas e fila de espera por livro;
- Confirmação da retirada do livro na loja (venda concretizada) e cancelamento de pedidos.
"""

import json
import os
import re
import unicodedata
from datetime import datetime, timezone
from decimal import Decimal

import boto3
from botocore.exceptions import ClientError

# Configuração do ambiente e conexão com o DynamoDB
REGIAO_AWS = os.environ.get("AWS_REGION", "us-east-1")
NOME_TABELA_LIVROS = os.environ.get("TABLE_LIVROS", "SeboLivros")
NOME_TABELA_PEDIDOS = os.environ.get("TABLE_PEDIDOS", "SeboPedidos")

dynamodb = boto3.resource("dynamodb", region_name=REGIAO_AWS)

tabela_livros = dynamodb.Table(NOME_TABELA_LIVROS)
tabela_pedidos = dynamodb.Table(NOME_TABELA_PEDIDOS)

# Status em que o exemplar já está separado para o cliente, aguardando a retirada na loja
STATUS_EXEMPLAR_SEPARADO = ("DISPONIVEL_NOTIFICADO", "CHEGOU_NOTIFICADO")

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
    """Gera um identificador padronizado a partir do título (ex: 'O Alienista' -> 'o-alienista')."""
    sem_acentos = unicodedata.normalize("NFKD", texto).encode("ascii", "ignore").decode("ascii")
    slug = re.sub(r"[^a-z0-9]+", "-", sem_acentos.lower()).strip("-")
    return slug or "livro-sem-id"


def agora_iso() -> str:
    """Retorna o timestamp atual em UTC no formato ISO 8601."""
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def escanear_tabela_completa(tabela) -> list:
    """Lê todos os itens de uma tabela DynamoDB percorrendo todas as páginas (LastEvaluatedKey)."""
    itens = []
    parametros = {}
    while True:
        resposta = tabela.scan(**parametros)
        itens.extend(resposta.get("Items", []))
        chave_continuacao = resposta.get("LastEvaluatedKey")
        if not chave_continuacao:
            break
        parametros["ExclusiveStartKey"] = chave_continuacao
    return itens


def cadastrar_ou_repor_livro(dados: dict) -> dict:
    """
    Cadastra um novo livro no acervo ou soma exemplares ao estoque existente.
    Se 'autor' ou 'preco' não forem informados na reposição, preserva os valores já salvos no banco.
    A alteração na tabela SeboLivros aciona automaticamente o DynamoDB Stream para a Lambda 3.
    """
    titulo = (dados.get("titulo") or "").strip()
    livro_id = (dados.get("livro_id") or "").strip() or normalizar_livro_id(titulo)
    autor_informado = (dados.get("autor") or "").strip()
    preco_informado = str(dados.get("preco") if dados.get("preco") is not None else "").strip()

    try:
        quantidade_adicional = int(dados.get("quantidade_adicional", dados.get("quantidade", 1)))
    except (ValueError, TypeError):
        return montar_resposta(400, {
            "sucesso": False,
            "mensagem": "O campo 'quantidade_adicional' deve ser um número inteiro.",
        })

    if not livro_id or not titulo:
        return montar_resposta(400, {
            "sucesso": False,
            "mensagem": "Informe pelo menos o 'titulo' do livro.",
        })

    if quantidade_adicional < 0:
        return montar_resposta(400, {
            "sucesso": False,
            "mensagem": "A quantidade adicionada não pode ser negativa.",
        })

    momento_atual = agora_iso()

    # Preserva titulo, autor e preco existentes quando o colaborador faz apenas reposição de quantidade
    expressao_titulo = "titulo = if_not_exists(titulo, :titulo)"
    expressao_autor = "autor = :autor" if autor_informado else "autor = if_not_exists(autor, :autor_padrao)"
    expressao_preco = "preco = :preco" if preco_informado else "preco = if_not_exists(preco, :preco_padrao)"

    valores_expressao = {
        ":titulo": titulo,
        ":qtd": quantidade_adicional,
        ":zero": 0,
        ":agora": momento_atual,
    }
    if autor_informado:
        valores_expressao[":autor"] = autor_informado
    else:
        valores_expressao[":autor_padrao"] = "Autor Desconhecido"

    if preco_informado:
        valores_expressao[":preco"] = preco_informado
    else:
        valores_expressao[":preco_padrao"] = "0.00"

    resposta = tabela_livros.update_item(
        Key={"livro_id": livro_id},
        UpdateExpression=(
            f"SET {expressao_titulo}, "
            f"{expressao_autor}, "
            f"{expressao_preco}, "
            "quantidade = if_not_exists(quantidade, :zero) + :qtd, "
            "total_vendidos = if_not_exists(total_vendidos, :zero), "
            "atualizado_em = :agora"
        ),
        ExpressionAttributeValues=valores_expressao,
        ReturnValues="ALL_NEW",
    )

    livro_atualizado = resposta.get("Attributes", {})
    titulo_exibicao = livro_atualizado.get("titulo", titulo)

    return montar_resposta(200, {
        "sucesso": True,
        "mensagem": (
            f"Estoque atualizado com sucesso para '{titulo_exibicao}'. "
            f"Foram adicionadas {quantidade_adicional} unidade(s). "
            f"Caso existam clientes na fila de espera, eles serão notificados automaticamente."
        ),
        "livro": livro_atualizado,
    })


def finalizar_pedido(pedido_id: str, status_final: str, status_permitidos: tuple,
                     campo_momento: str, observacao: str) -> dict | None:
    """
    Muda o status de um pedido para 'status_final' somente se o status atual estiver em 'status_permitidos'.
    A condição é verificada pelo DynamoDB, então o mesmo pedido não é finalizado duas vezes.
    Retorna o pedido como era antes da alteração, ou None se ele não existe ou não pode mais ser alterado.
    """
    valores = {f":permitido{i}": status for i, status in enumerate(status_permitidos)}
    valores.update({":status_final": status_final, ":momento": agora_iso(), ":obs": observacao})

    try:
        resposta = tabela_pedidos.update_item(
            Key={"pedido_id": pedido_id},
            UpdateExpression="SET #status = :status_final, #momento = :momento, observacao = :obs",
            ConditionExpression=f"#status IN ({', '.join(k for k in valores if k.startswith(':permitido'))})",
            ExpressionAttributeNames={"#status": "status", "#momento": campo_momento},
            ExpressionAttributeValues=valores,
            ReturnValues="ALL_OLD",
        )
        return resposta.get("Attributes")
    except ClientError as erro:
        if erro.response["Error"]["Code"] == "ConditionalCheckFailedException":
            return None
        raise


def devolver_exemplar_ao_estoque(livro_id: str) -> None:
    """
    Devolve 1 unidade ao estoque de um livro (reserva cancelada).
    A alteração na tabela SeboLivros aciona o DynamoDB Stream, e a Lambda 3 oferece o exemplar
    ao próximo cliente da fila de espera, se houver.
    """
    tabela_livros.update_item(
        Key={"livro_id": livro_id},
        UpdateExpression=(
            "SET quantidade = quantidade + :um, "
            "total_vendidos = total_vendidos - :um, "
            "atualizado_em = :agora"
        ),
        ExpressionAttributeValues={":um": 1, ":agora": agora_iso()},
    )


def confirmar_retirada(dados: dict) -> dict:
    """Registra que o cliente compareceu à livraria, retirou o exemplar separado e concretizou a compra."""
    pedido_id = (dados.get("pedido_id") or "").strip()
    if not pedido_id:
        return montar_resposta(400, {"sucesso": False, "mensagem": "Informe o 'pedido_id'."})

    pedido_anterior = finalizar_pedido(
        pedido_id,
        status_final="RETIRADO",
        status_permitidos=STATUS_EXEMPLAR_SEPARADO,
        campo_momento="retirado_em",
        observacao="Cliente retirou o livro na loja. Venda concretizada.",
    )
    if pedido_anterior is None:
        return montar_resposta(409, {
            "sucesso": False,
            "mensagem": (
                f"O pedido '{pedido_id}' não existe ou não tem exemplar separado aguardando retirada "
                f"(só é possível confirmar pedidos com status {' ou '.join(STATUS_EXEMPLAR_SEPARADO)})."
            ),
        })

    return montar_resposta(200, {
        "sucesso": True,
        "mensagem": f"Retirada do pedido {pedido_id} ('{pedido_anterior.get('titulo', '')}') confirmada. Venda concretizada.",
        "pedido_id": pedido_id,
        "status": "RETIRADO",
    })


def cancelar_pedido(dados: dict) -> dict:
    """
    Cancela um pedido que ainda não foi retirado.
    - Se o exemplar estava separado, ele volta ao estoque (e o próximo da fila é atendido automaticamente);
    - Se o pedido estava na fila de espera, apenas sai da fila.
    """
    pedido_id = (dados.get("pedido_id") or "").strip()
    if not pedido_id:
        return montar_resposta(400, {"sucesso": False, "mensagem": "Informe o 'pedido_id'."})

    pedido_anterior = finalizar_pedido(
        pedido_id,
        status_final="CANCELADO",
        status_permitidos=STATUS_EXEMPLAR_SEPARADO + ("AGUARDANDO_ESTOQUE",),
        campo_momento="cancelado_em",
        observacao="Pedido cancelado pelo sebo.",
    )
    if pedido_anterior is None:
        return montar_resposta(409, {
            "sucesso": False,
            "mensagem": f"O pedido '{pedido_id}' não existe ou já foi retirado/cancelado.",
        })

    exemplar_devolvido = pedido_anterior.get("status") in STATUS_EXEMPLAR_SEPARADO
    if exemplar_devolvido:
        devolver_exemplar_ao_estoque(pedido_anterior["livro_id"])
        mensagem = (
            f"Pedido {pedido_id} cancelado. O exemplar de '{pedido_anterior.get('titulo', '')}' voltou ao estoque "
            f"e será oferecido automaticamente ao próximo cliente da fila de espera, se houver."
        )
    else:
        mensagem = f"Pedido {pedido_id} cancelado e removido da fila de espera."

    return montar_resposta(200, {
        "sucesso": True,
        "mensagem": mensagem,
        "pedido_id": pedido_id,
        "status": "CANCELADO",
        "exemplar_devolvido": exemplar_devolvido,
    })


def consultar_painel_estoque() -> dict:
    """
    Retorna o panorama completo do sebo para o colaborador:
    - Lista de livros e quantidades em estoque;
    - Lista de pedidos (vendas imediatas, atendidos posteriormente e fila de espera);
    - Resumo numérico de estoque, vendas e encomendas pendentes.
    """
    livros = escanear_tabela_completa(tabela_livros)
    pedidos = escanear_tabela_completa(tabela_pedidos)

    livros.sort(key=lambda item: item.get("titulo", ""))
    pedidos.sort(key=lambda item: item.get("criado_em", ""), reverse=True)

    # Conta quantos clientes estão aguardando cada livro na fila de espera
    fila_por_livro = {}
    for pedido in pedidos:
        if pedido.get("status") == "AGUARDANDO_ESTOQUE":
            lid = pedido.get("livro_id", "")
            fila_por_livro[lid] = fila_por_livro.get(lid, 0) + 1

    for livro in livros:
        lid = livro.get("livro_id", "")
        livro["clientes_aguardando"] = fila_por_livro.get(lid, 0)

    def contar(*status) -> int:
        return sum(1 for p in pedidos if p.get("status") in status)

    resumo = {
        "total_titulos": len(livros),
        "total_exemplares_disponiveis": sum(int(l.get("quantidade", 0)) for l in livros),
        "total_vendas_reservadas": contar(*STATUS_EXEMPLAR_SEPARADO, "RETIRADO"),
        "total_aguardando_retirada": contar(*STATUS_EXEMPLAR_SEPARADO),
        "total_retirados": contar("RETIRADO"),
        "total_cancelados": contar("CANCELADO"),
        "total_fila_espera": contar("AGUARDANDO_ESTOQUE"),
    }

    return montar_resposta(200, {
        "sucesso": True,
        "resumo": resumo,
        "livros": livros,
        "pedidos": pedidos,
    })


def lambda_handler(event, context):
    """Ponto de entrada da função Lambda: roteia a requisição do colaborador."""
    metodo = extrair_metodo_http(event)

    if metodo == "OPTIONS":
        return montar_resposta(200, {"ok": True})

    try:
        dados = extrair_dados_evento(event)
        acao = (dados.get("acao") or "").strip().lower()

        if metodo == "GET" and not acao:
            acao = "painel_estoque"

        if acao == "cadastrar_ou_repor_livro":
            return cadastrar_ou_repor_livro(dados)
        elif acao == "painel_estoque":
            return consultar_painel_estoque()
        elif acao == "confirmar_retirada":
            return confirmar_retirada(dados)
        elif acao == "cancelar_pedido":
            return cancelar_pedido(dados)
        else:
            return montar_resposta(400, {
                "sucesso": False,
                "mensagem": (
                    f"Ação '{acao}' inválida. Use: 'cadastrar_ou_repor_livro', 'painel_estoque', "
                    f"'confirmar_retirada' ou 'cancelar_pedido'."
                ),
            })

    except Exception as erro:
        print(f"[ERRO LAMBDA COLABORADOR] {erro}")
        return montar_resposta(500, {
            "sucesso": False,
            "mensagem": f"Erro interno na Lambda do Colaborador: {str(erro)}",
        })
