"""
Teste Automatizado Ponta-a-Ponta dos 3 Fluxos de Negócio (test_fluxos.py)

Valida os três cenários exigidos para o Sebo Serverless:
- Fluxo 1: Compra imediata de livro com estoque disponível (quantidade > 0) -> status DISPONIVEL_NOTIFICADO + e-mail SNS;
- Fluxo 2: Interesse em livro sem estoque (quantidade == 0) -> inclusão na fila de espera com status AGUARDANDO_ESTOQUE;
- Fluxo 3: Reposição de estoque pelo colaborador -> gatilho do DynamoDB Stream aciona a Lambda 3 ->
           reserva automática do exemplar, status atualizado para CHEGOU_NOTIFICADO e envio de e-mail SNS.
- Fluxo 4: Colaborador confirma a retirada de um pedido (RETIRADO) e cancela outro (CANCELADO) ->
           o exemplar devolvido aciona o Stream e o próximo cliente da fila é atendido automaticamente.

Modos de execução:
1. Modo Local (--local): Simula o DynamoDB, o DynamoDB Stream e o SNS em memória e invoca
   diretamente os handlers das 3 Lambdas (não requer conexão com a AWS).
2. Modo AWS Real (padrão): Consome as Function URLs configuradas no arquivo .env.

Uso:
  python scripts/test_fluxos.py --local
  python scripts/test_fluxos.py
"""

import argparse
import copy
import importlib.util
import json
import os
import re
import sys
import time
from decimal import Decimal
from pathlib import Path
from unittest.mock import MagicMock

try:
    import requests
except ImportError:
    requests = None

from botocore.exceptions import ClientError

try:
    from dotenv import load_dotenv
except ImportError:
    def load_dotenv(*args, **kwargs):
        return False

RAIZ_PROJETO = Path(__file__).resolve().parent.parent


# ==============================================================================
# 1. SIMULADOR EM MEMÓRIA DO DYNAMODB + STREAM + SNS (MODO --local)
# ==============================================================================
def erro_condicional_dynamodb() -> ClientError:
    """Gera a exceção padrão do boto3 quando uma ConditionExpression falha."""
    return ClientError(
        {
            "Error": {
                "Code": "ConditionalCheckFailedException",
                "Message": "The conditional request failed",
            }
        },
        "UpdateItem",
    )


def serializar_para_stream(item: dict) -> dict:
    """Converte um dicionário Python para o formato tipado do DynamoDB Stream (ex: {'S': ...}, {'N': ...})."""
    imagem = {}
    for chave, valor in item.items():
        if valor is None:
            imagem[chave] = {"NULL": True}
        elif isinstance(valor, bool):
            imagem[chave] = {"BOOL": valor}
        elif isinstance(valor, (int, float, Decimal)):
            imagem[chave] = {"N": str(valor)}
        else:
            imagem[chave] = {"S": str(valor)}
    return imagem


class TabelaDynamoDBMemoria:
    """Simula em memória as operações de uma tabela DynamoDB (get_item, put_item, update_item, scan)."""

    def __init__(self, nome: str, chave_primaria: str, callback_stream=None):
        self.nome = nome
        self.chave_primaria = chave_primaria
        self.itens = {}
        self.callback_stream = callback_stream

    def get_item(self, Key: dict, **kwargs) -> dict:
        valor_pk = Key[self.chave_primaria]
        if valor_pk in self.itens:
            return {"Item": copy.deepcopy(self.itens[valor_pk])}
        return {}

    def put_item(self, Item: dict, ConditionExpression: str | None = None, **kwargs) -> dict:
        valor_pk = Item[self.chave_primaria]
        if ConditionExpression and "attribute_not_exists" in ConditionExpression:
            if valor_pk in self.itens:
                raise erro_condicional_dynamodb()

        imagem_antiga = copy.deepcopy(self.itens.get(valor_pk))
        self.itens[valor_pk] = copy.deepcopy(Item)
        self._disparar_stream("INSERT" if imagem_antiga is None else "MODIFY", imagem_antiga, self.itens[valor_pk])
        return {}

    def scan(self, **kwargs) -> dict:
        lista = [copy.deepcopy(v) for v in self.itens.values()]
        return {"Items": lista}

    def update_item(
        self,
        Key: dict,
        UpdateExpression: str,
        ConditionExpression: str | None = None,
        ExpressionAttributeValues: dict | None = None,
        ExpressionAttributeNames: dict | None = None,
        ReturnValues: str | None = None,
    ) -> dict:
        valor_pk = Key[self.chave_primaria]
        valores = ExpressionAttributeValues or {}
        item_atual = copy.deepcopy(self.itens.get(valor_pk, {self.chave_primaria: valor_pk}))
        existia = valor_pk in self.itens

        # Avalia as ConditionExpressions utilizadas nas Lambdas 1 e 3
        if ConditionExpression:
            if "quantidade > :zero" in ConditionExpression:
                qtd_atual = int(item_atual.get("quantidade", 0))
                if not existia or qtd_atual <= int(valores.get(":zero", 0)):
                    raise erro_condicional_dynamodb()
            if "#status = :aguardando" in ConditionExpression:
                if item_atual.get("status") != valores.get(":aguardando"):
                    raise erro_condicional_dynamodb()
            if "#status IN (" in ConditionExpression:
                permitidos = [valores[k] for k in re.findall(r":\w+", ConditionExpression.split("IN", 1)[1])]
                if not existia or item_atual.get("status") not in permitidos:
                    raise erro_condicional_dynamodb()

        # Aplica as UpdateExpressions utilizadas nas Lambdas 1, 2 e 3
        if "quantidade = quantidade - :um" in UpdateExpression:
            item_atual["quantidade"] = int(item_atual.get("quantidade", 0)) - int(valores[":um"])
            item_atual["total_vendidos"] = int(item_atual.get("total_vendidos", 0)) + int(valores[":um"])
            item_atual["atualizado_em"] = valores[":agora"]

        elif "quantidade = quantidade + :um" in UpdateExpression:
            item_atual["quantidade"] = int(item_atual.get("quantidade", 0)) + int(valores[":um"])
            item_atual["total_vendidos"] = int(item_atual.get("total_vendidos", 0)) - int(valores[":um"])
            item_atual["atualizado_em"] = valores[":agora"]

        elif "quantidade = if_not_exists(quantidade, :zero) + :qtd" in UpdateExpression:
            item_atual["titulo"] = item_atual.get("titulo") or valores[":titulo"]
            if ":autor" in valores:
                item_atual["autor"] = valores[":autor"]
            else:
                item_atual["autor"] = item_atual.get("autor") or valores.get(":autor_padrao", "Autor Desconhecido")
            if ":preco" in valores:
                item_atual["preco"] = valores[":preco"]
            else:
                item_atual["preco"] = item_atual.get("preco") or valores.get(":preco_padrao", "0.00")
            item_atual["quantidade"] = int(item_atual.get("quantidade", valores[":zero"])) + int(valores[":qtd"])
            item_atual["total_vendidos"] = int(item_atual.get("total_vendidos", valores[":zero"]))
            item_atual["atualizado_em"] = valores[":agora"]

        elif "#status = :status_final" in UpdateExpression:
            item_atual["status"] = valores[":status_final"]
            item_atual[(ExpressionAttributeNames or {})["#momento"]] = valores[":momento"]
            item_atual["observacao"] = valores[":obs"]

        elif "#status = :novo" in UpdateExpression:
            item_atual["status"] = valores[":novo"]
            item_atual["notificado_em"] = valores[":momento"]
            item_atual["observacao"] = valores[":obs"]

        elif "notificado_em = :nulo" in UpdateExpression:
            item_atual["notificado_em"] = valores[":nulo"]
            item_atual["observacao"] = valores[":obs"]

        imagem_antiga = copy.deepcopy(self.itens.get(valor_pk))
        self.itens[valor_pk] = item_atual
        self._disparar_stream("MODIFY" if existia else "INSERT", imagem_antiga, item_atual)
        if ReturnValues == "ALL_OLD":
            return {"Attributes": copy.deepcopy(imagem_antiga)} if imagem_antiga else {}
        return {"Attributes": copy.deepcopy(item_atual)}

    def _disparar_stream(self, tipo_evento: str, imagem_antiga: dict | None, imagem_nova: dict) -> None:
        if not self.callback_stream:
            return
        evento_stream = {
            "Records": [
                {
                    "eventName": tipo_evento,
                    "dynamodb": {
                        "OldImage": serializar_para_stream(imagem_antiga) if imagem_antiga else {},
                        "NewImage": serializar_para_stream(imagem_nova),
                    },
                }
            ]
        }
        self.callback_stream(evento_stream)


def carregar_modulo_lambda(nome_modulo: str, caminho_arquivo: Path):
    """Importa dinamicamente o arquivo lambda_function.py de uma das pastas lambdas/."""
    spec = importlib.util.spec_from_file_location(nome_modulo, caminho_arquivo)
    modulo = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(modulo)
    return modulo


def executar_teste_local() -> None:
    """Executa os 3 fluxos de negócio localmente simulando DynamoDB, Stream e SNS em memória."""
    print("=" * 74)
    print("  TESTE AUTOMATIZADO LOCAL (MOCK EM MEMÓRIA DAS 3 LAMBDAS + STREAM + SNS)")
    print("=" * 74)

    os.environ["SNS_TOPIC_ARN"] = "arn:aws:sns:us-east-1:123456789012:SeboNotificacoes"

    l1 = carregar_modulo_lambda("lambda1", RAIZ_PROJETO / "lambdas" / "lambda1_comprador" / "lambda_function.py")
    l2 = carregar_modulo_lambda("lambda2", RAIZ_PROJETO / "lambdas" / "lambda2_colaborador" / "lambda_function.py")
    l3 = carregar_modulo_lambda("lambda3", RAIZ_PROJETO / "lambdas" / "lambda3_notificador" / "lambda_function.py")

    emails_publicados_sns = []
    sns_mock = MagicMock()

    def registrar_publish(**kwargs):
        emails_publicados_sns.append(kwargs)
        return {"MessageId": f"MSG-{len(emails_publicados_sns)}"}

    sns_mock.publish.side_effect = registrar_publish

    # Substitui buscar_fila_de_espera na Lambda 3 para filtrar em memória durante o mock local
    tabela_pedidos_mem = TabelaDynamoDBMemoria("SeboPedidos", "pedido_id")

    def buscar_fila_memoria(livro_id: str) -> list:
        pedidos = [
            copy.deepcopy(p)
            for p in tabela_pedidos_mem.itens.values()
            if p.get("livro_id") == livro_id and p.get("status") == "AGUARDANDO_ESTOQUE"
        ]
        pedidos.sort(key=lambda p: p.get("criado_em", ""))
        return pedidos

    l3.buscar_fila_de_espera = buscar_fila_memoria

    # Evita recursão do stream quando a própria Lambda 3 decrementa o estoque
    em_execucao_stream = {"ativo": False}

    def acionar_lambda3_via_stream(evento_stream: dict):
        if em_execucao_stream["ativo"]:
            return
        em_execucao_stream["ativo"] = True
        try:
            l3.lambda_handler(evento_stream, None)
        finally:
            em_execucao_stream["ativo"] = False

    tabela_livros_mem = TabelaDynamoDBMemoria("SeboLivros", "livro_id", callback_stream=acionar_lambda3_via_stream)

    # Conecta as tabelas em memória e o mock do SNS nas 3 Lambdas
    for modulo in (l1, l2, l3):
        modulo.tabela_livros = tabela_livros_mem
        modulo.tabela_pedidos = tabela_pedidos_mem
    l1.sns_client = sns_mock
    l3.sns_client = sns_mock

    # Pré-carrega 1 exemplar de "Dom Casmurro" para testar o Fluxo 1
    l2.lambda_handler(
        {
            "acao": "cadastrar_ou_repor_livro",
            "titulo": "Dom Casmurro",
            "autor": "Machado de Assis",
            "preco": "25.00",
            "quantidade_adicional": 1,
        },
        None,
    )

    # --------------------------------------------------------------------------
    # FLUXO 1: Compra de livro com estoque disponível (Dom Casmurro -> qtd = 1)
    # --------------------------------------------------------------------------
    print("\n[FLUXO 1] Comprador solicita 'Dom Casmurro' (estoque atual = 1)...")
    resp_f1 = l1.lambda_handler(
        {
            "acao": "solicitar_livro",
            "titulo": "Dom Casmurro",
            "comprador_nome": "Hugo Freires",
            "comprador_email": "hugo@exemplo.com",
        },
        None,
    )
    corpo_f1 = json.loads(resp_f1["body"])
    assert corpo_f1["sucesso"] is True and corpo_f1["disponivel_imediato"] is True
    assert corpo_f1["pedido"]["status"] == "DISPONIVEL_NOTIFICADO"
    assert tabela_livros_mem.itens["dom-casmurro"]["quantidade"] == 0
    assert len(emails_publicados_sns) == 1
    print(f"  -> [PASSOU] Pedido {corpo_f1['pedido']['pedido_id']} criado como DISPONIVEL_NOTIFICADO.")
    print(f"  -> [PASSOU] Estoque de 'Dom Casmurro' caiu de 1 para 0.")
    print(f"  -> [PASSOU] E-mail SNS enviado imediatamente: '{emails_publicados_sns[-1]['Subject']}'")

    # --------------------------------------------------------------------------
    # FLUXO 2: Interesse em livro sem estoque (Dom Casmurro agora tem qtd = 0)
    # --------------------------------------------------------------------------
    print("\n[FLUXO 2] Segundo comprador solicita 'Dom Casmurro' (estoque atual = 0)...")
    resp_f2 = l1.lambda_handler(
        {
            "acao": "solicitar_livro",
            "titulo": "Dom Casmurro",
            "comprador_nome": "Maria Silva",
            "comprador_email": "maria@exemplo.com",
        },
        None,
    )
    corpo_f2 = json.loads(resp_f2["body"])
    pedido_fila_id = corpo_f2["pedido"]["pedido_id"]
    assert corpo_f2["sucesso"] is True and corpo_f2["disponivel_imediato"] is False
    assert corpo_f2["pedido"]["status"] == "AGUARDANDO_ESTOQUE"
    assert len(emails_publicados_sns) == 1  # Nenhum e-mail novo ainda
    print(f"  -> [PASSOU] Pedido {pedido_fila_id} salvo na fila de espera com status AGUARDANDO_ESTOQUE.")

    # --------------------------------------------------------------------------
    # FLUXO 3: Colaborador repõe +1 exemplar -> Stream aciona Lambda 3 -> Notifica Maria
    # --------------------------------------------------------------------------
    print("\n[FLUXO 3] Colaborador registra chegada de +1 exemplar de 'Dom Casmurro'...")
    resp_f3 = l2.lambda_handler(
        {
            "acao": "cadastrar_ou_repor_livro",
            "titulo": "Dom Casmurro",
            "autor": "Machado de Assis",
            "preco": "25.00",
            "quantidade_adicional": 1,
        },
        None,
    )
    corpo_f3 = json.loads(resp_f3["body"])
    assert corpo_f3["sucesso"] is True
    pedido_atualizado = tabela_pedidos_mem.itens[pedido_fila_id]
    assert pedido_atualizado["status"] == "CHEGOU_NOTIFICADO"
    assert tabela_livros_mem.itens["dom-casmurro"]["quantidade"] == 0  # Já reservado para a Maria!
    assert len(emails_publicados_sns) == 2
    print(f"  -> [PASSOU] DynamoDB Stream acionou a Lambda 3 automaticamente!")
    print(f"  -> [PASSOU] Pedido {pedido_fila_id} mudou de AGUARDANDO_ESTOQUE para CHEGOU_NOTIFICADO.")
    print(f"  -> [PASSOU] Exemplar recém-chegado foi alocado para Maria (estoque livre voltou para 0).")
    print(f"  -> [PASSOU] E-mail SNS de chegada enviado: '{emails_publicados_sns[-1]['Subject']}'")

    # --------------------------------------------------------------------------
    # FLUXO 4: Retirada na loja e cancelamento -> exemplar devolvido atende o próximo da fila
    # --------------------------------------------------------------------------
    print("\n[FLUXO 4] Retirada na loja e cancelamento de pedido...")
    pedido_hugo_id = corpo_f1["pedido"]["pedido_id"]
    corpo_joana = json.loads(l1.lambda_handler(
        {
            "acao": "solicitar_livro",
            "titulo": "Dom Casmurro",
            "comprador_nome": "Joana Lima",
            "comprador_email": "joana@exemplo.com",
        },
        None,
    )["body"])
    pedido_joana_id = corpo_joana["pedido"]["pedido_id"]
    assert corpo_joana["pedido"]["status"] == "AGUARDANDO_ESTOQUE"

    resp_retirada = l2.lambda_handler({"acao": "confirmar_retirada", "pedido_id": pedido_hugo_id}, None)
    assert resp_retirada["statusCode"] == 200
    assert tabela_pedidos_mem.itens[pedido_hugo_id]["status"] == "RETIRADO"
    assert tabela_livros_mem.itens["dom-casmurro"]["quantidade"] == 0
    print(f"  -> [PASSOU] Retirada do pedido {pedido_hugo_id} confirmada (status RETIRADO, estoque inalterado).")

    emails_antes = len(emails_publicados_sns)
    corpo_cancel = json.loads(l2.lambda_handler({"acao": "cancelar_pedido", "pedido_id": pedido_fila_id}, None)["body"])
    assert corpo_cancel["sucesso"] is True and corpo_cancel["exemplar_devolvido"] is True
    assert tabela_pedidos_mem.itens[pedido_fila_id]["status"] == "CANCELADO"
    assert tabela_pedidos_mem.itens[pedido_joana_id]["status"] == "CHEGOU_NOTIFICADO"
    assert tabela_livros_mem.itens["dom-casmurro"]["quantidade"] == 0
    assert tabela_livros_mem.itens["dom-casmurro"]["total_vendidos"] == 2
    assert len(emails_publicados_sns) == emails_antes + 1
    print(f"  -> [PASSOU] Pedido {pedido_fila_id} cancelado; o exemplar voltou ao estoque.")
    print(f"  -> [PASSOU] Stream atendeu automaticamente o próximo da fila ({pedido_joana_id} -> CHEGOU_NOTIFICADO).")

    resp_invalida = l2.lambda_handler({"acao": "confirmar_retirada", "pedido_id": pedido_fila_id}, None)
    assert resp_invalida["statusCode"] == 409
    print("  -> [PASSOU] Não é possível confirmar a retirada de um pedido cancelado (HTTP 409).")

    print("\n" + "=" * 74)
    print("  RESULTADO FINAL: TODOS OS 4 FLUXOS PASSARAM COM 100% DE SUCESSO!")
    print("=" * 74)


# ==============================================================================
# 2. TESTE NAS FUNCTION URLs REAIS DA AWS
# ==============================================================================
def executar_teste_aws(url_comprador: str, url_colaborador: str) -> None:
    """Executa os 3 fluxos chamando as Function URLs reais implantadas na AWS."""
    print("=" * 74)
    print("  TESTE PONTA-A-PONTA NAS FUNCTION URLs REAIS DA AWS")
    print("=" * 74)

    titulo_teste = "O Alienista"

    print(f"\n1. Colaborador abastece +1 unidade de '{titulo_teste}'...")
    r_cadastro = requests.post(
        url_colaborador,
        json={
            "acao": "cadastrar_ou_repor_livro",
            "titulo": titulo_teste,
            "autor": "Machado de Assis",
            "preco": "20.00",
            "quantidade_adicional": 1,
        },
        timeout=20,
    ).json()
    print(f"   Resposta Lambda 2: {r_cadastro.get('mensagem')}")

    print(f"\n2. [FLUXO 1] Comprador 1 solicita '{titulo_teste}' (disponível em estoque)...")
    r_compra1 = requests.post(
        url_comprador,
        json={
            "acao": "solicitar_livro",
            "titulo": titulo_teste,
            "comprador_nome": "Cliente Com Estoque",
            "comprador_email": "cliente1@exemplo.com",
        },
        timeout=20,
    ).json()
    print(f"   Status do Pedido: {r_compra1.get('pedido', {}).get('status')}")
    print(f"   Mensagem: {r_compra1.get('mensagem')}")

    print(f"\n3. [FLUXO 2] Comprador 2 solicita '{titulo_teste}' (agora sem estoque)...")
    r_compra2 = requests.post(
        url_comprador,
        json={
            "acao": "solicitar_livro",
            "titulo": titulo_teste,
            "comprador_nome": "Cliente Fila de Espera",
            "comprador_email": "cliente2@exemplo.com",
        },
        timeout=20,
    ).json()
    pedido_espera_id = r_compra2.get("pedido", {}).get("pedido_id")
    print(f"   Pedido na Fila: {pedido_espera_id} | Status: {r_compra2.get('pedido', {}).get('status')}")

    print(f"\n4. [FLUXO 3] Colaborador repõe +1 unidade de '{titulo_teste}' para acionar o DynamoDB Stream...")
    requests.post(
        url_colaborador,
        json={
            "acao": "cadastrar_ou_repor_livro",
            "titulo": titulo_teste,
            "autor": "Machado de Assis",
            "preco": "20.00",
            "quantidade_adicional": 1,
        },
        timeout=20,
    )
    print("   Aguardando 5 segundos para o DynamoDB Stream acionar a Lambda 3...")
    time.sleep(5)

    r_pedidos = requests.post(
        url_comprador,
        json={"acao": "meus_pedidos", "comprador_email": "cliente2@exemplo.com"},
        timeout=20,
    ).json()
    pedidos_cliente2 = r_pedidos.get("pedidos", [])
    pedido_atendido = next((p for p in pedidos_cliente2 if p.get("pedido_id") == pedido_espera_id), {})
    print(f"   Status atualizado do Pedido {pedido_espera_id}: {pedido_atendido.get('status')}")

    pedido_compra1_id = r_compra1.get("pedido", {}).get("pedido_id")
    print(f"\n5. [FLUXO 4] Colaborador confirma a retirada do pedido {pedido_compra1_id} na loja...")
    r_retirada = requests.post(
        url_colaborador,
        json={"acao": "confirmar_retirada", "pedido_id": pedido_compra1_id},
        timeout=20,
    ).json()
    print(f"   Resposta Lambda 2: {r_retirada.get('mensagem')}")
    print("\n" + "=" * 74)


def main():
    parser = argparse.ArgumentParser(description="Testa os fluxos de negócio do Sebo Serverless.")
    parser.add_argument(
        "--local",
        action="store_true",
        help="Executa o teste localmente simulando DynamoDB, Stream e SNS em memória.",
    )
    args = parser.parse_args()

    if args.local:
        executar_teste_local()
        return

    load_dotenv(RAIZ_PROJETO / ".env")
    load_dotenv()
    url_comprador = os.environ.get("LAMBDA_COMPRADOR_URL", "").strip()
    url_colaborador = os.environ.get("LAMBDA_COLABORADOR_URL", "").strip()

    if not url_comprador or not url_colaborador or "xxxx" in url_comprador:
        print(
            "[AVISO] Function URLs da AWS ainda não configuradas no .env.\n"
            "Executando automaticamente em modo de simulação local (--local):\n"
        )
        executar_teste_local()
        return

    executar_teste_aws(url_comprador, url_colaborador)


if __name__ == "__main__":
    main()
