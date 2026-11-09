"""
Serviço do Colaborador (sebo-colaborador-service)

Gerencia as operações internas da livraria (sebo):
- Cadastro de novos livros e reposição de exemplares em estoque;
- Painel consolidado com catálogo, histórico de vendas e fila de espera por livro.
"""

import json
import os
import re
import unicodedata
from datetime import datetime, timezone
from decimal import Decimal

import boto3

# Configuração do ambiente e conexão com o DynamoDB
REGIAO_AWS = os.environ.get("AWS_REGION", "us-east-1")
NOME_TABELA_LIVROS = os.environ.get("TABLE_LIVROS", "SeboLivros")
NOME_TABELA_PEDIDOS = os.environ.get("TABLE_PEDIDOS", "SeboPedidos")

dynamodb = boto3.resource("dynamodb", region_name=REGIAO_AWS)

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
    """Gera um identificador padronizado a partir do título (ex: 'O Alienista' -> 'o-alienista')."""
    sem_acentos = unicodedata.normalize("NFKD", texto).encode("ascii", "ignore").decode("ascii")
    slug = re.sub(r"[^a-z0-9]+", "-", sem_acentos.lower()).strip("-")
    return slug or "livro-sem-id"


def agora_iso() -> str:
    """Retorna o timestamp atual em UTC no formato ISO 8601."""
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def cadastrar_ou_repor_livro(dados: dict) -> dict:
    """
    Cadastra um novo livro no acervo ou soma exemplares ao estoque existente.
    A alteração na tabela SeboLivros aciona automaticamente o DynamoDB Stream para a Lambda 3.
    """
    titulo = (dados.get("titulo") or "").strip()
    livro_id = (dados.get("livro_id") or "").strip() or normalizar_livro_id(titulo)
    autor = (dados.get("autor") or "Autor Desconhecido").strip()
    preco = str(dados.get("preco") or "0.00").strip()

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

    # Soma a quantidade_adicional ao estoque atual (ou inicializa em 0 se for livro novo)
    resposta = tabela_livros.update_item(
        Key={"livro_id": livro_id},
        UpdateExpression=(
            "SET titulo = :titulo, "
            "autor = :autor, "
            "preco = :preco, "
            "quantidade = if_not_exists(quantidade, :zero) + :qtd, "
            "total_vendidos = if_not_exists(total_vendidos, :zero), "
            "atualizado_em = :agora"
        ),
        ExpressionAttributeValues={
            ":titulo": titulo,
            ":autor": autor,
            ":preco": preco,
            ":qtd": quantidade_adicional,
            ":zero": 0,
            ":agora": momento_atual,
        },
        ReturnValues="ALL_NEW",
    )

    livro_atualizado = resposta.get("Attributes", {})

    return montar_resposta(200, {
        "sucesso": True,
        "mensagem": (
            f"Estoque atualizado com sucesso para '{titulo}'. "
            f"Foram adicionadas {quantidade_adicional} unidade(s). "
            f"Caso existam clientes na fila de espera, eles serão notificados automaticamente."
        ),
        "livro": livro_atualizado,
    })


def consultar_painel_estoque() -> dict:
    """
    Retorna o panorama completo do sebo para o colaborador:
    - Lista de livros e quantidades em estoque;
    - Lista de pedidos (vendas imediatas, atendidos posteriormente e fila de espera);
    - Resumo numérico de estoque, vendas e encomendas pendentes.
    """
    livros = tabela_livros.scan().get("Items", [])
    pedidos = tabela_pedidos.scan().get("Items", [])

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

    pedidos_aguardando = [p for p in pedidos if p.get("status") == "AGUARDANDO_ESTOQUE"]
    pedidos_atendidos = [p for p in pedidos if p.get("status") != "AGUARDANDO_ESTOQUE"]

    resumo = {
        "total_titulos": len(livros),
        "total_exemplares_disponiveis": sum(int(l.get("quantidade", 0)) for l in livros),
        "total_vendas_reservadas": len(pedidos_atendidos),
        "total_fila_espera": len(pedidos_aguardando),
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
        else:
            return montar_resposta(400, {
                "sucesso": False,
                "mensagem": f"Ação '{acao}' inválida. Use: 'cadastrar_ou_repor_livro' ou 'painel_estoque'.",
            })

    except Exception as erro:
        print(f"[ERRO LAMBDA COLABORADOR] {erro}")
        return montar_resposta(500, {
            "sucesso": False,
            "mensagem": f"Erro interno na Lambda do Colaborador: {str(erro)}",
        })
