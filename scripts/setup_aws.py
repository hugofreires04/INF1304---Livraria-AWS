"""
Script de Provisionamento Automatizado na AWS (setup_aws.py)

Cria ou atualiza todos os recursos necessários para o Sebo Serverless na conta AWS:
1. Tabelas DynamoDB: SeboLivros (com DynamoDB Stream NEW_AND_OLD_IMAGES) e SeboPedidos;
2. Tópico Amazon SNS: SeboNotificacoes (com opção de inscrever um e-mail via argumento);
3. Três funções AWS Lambda usando a role existente LabRole;
4. Function URLs públicas (com CORS) para as Lambdas do Comprador e do Colaborador;
5. Gatilho (Event Source Mapping) ligando o Stream de SeboLivros à Lambda Notificadora,
   com reprocessamento por registro (ReportBatchItemFailures), limite de tentativas e
   fila SQS de falhas (Dead-Letter Queue) para os registros que esgotarem as tentativas;
6. Gravação automática das Function URLs geradas no arquivo .env da raiz do projeto.

Uso:
  python scripts/setup_aws.py
  python scripts/setup_aws.py --email seu_email@exemplo.com
  python scripts/setup_aws.py --email seu_email@exemplo.com --filtrar-por-email
"""

import argparse
import io
import json
import os
import time
import zipfile
from pathlib import Path

import boto3
from botocore.exceptions import ClientError

RAIZ_PROJETO = Path(__file__).resolve().parent.parent
REGIAO_AWS = os.environ.get("AWS_REGION", "us-east-1")

NOME_TABELA_LIVROS = "SeboLivros"
NOME_TABELA_PEDIDOS = "SeboPedidos"
NOME_TOPICO_SNS = "SeboNotificacoes"
NOME_FILA_FALHAS = "SeboNotificadorFalhas"
MAXIMO_TENTATIVAS_STREAM = 3

CONFIG_LAMBDAS = [
    {
        "nome_funcao": "sebo-comprador-service",
        "descricao": "Atendimento ao Comprador: consulta acervo, compra imediata ou fila de espera",
        "arquivo_origem": RAIZ_PROJETO / "lambdas" / "lambda1_comprador" / "lambda_function.py",
        "criar_url": True,
        "env_var_url": "LAMBDA_COMPRADOR_URL",
    },
    {
        "nome_funcao": "sebo-colaborador-service",
        "descricao": "Operacao do Colaborador: cadastro/reposicao de estoque e painel gerencial",
        "arquivo_origem": RAIZ_PROJETO / "lambdas" / "lambda2_colaborador" / "lambda_function.py",
        "criar_url": True,
        "env_var_url": "LAMBDA_COLABORADOR_URL",
    },
    {
        "nome_funcao": "sebo-notificador-estoque",
        "descricao": "Worker reativo acionado pelo DynamoDB Stream para atender fila de espera e avisar via SNS",
        "arquivo_origem": RAIZ_PROJETO / "lambdas" / "lambda3_notificador" / "lambda_function.py",
        "criar_url": False,
        "env_var_url": None,
    },
]


def obter_lab_role_arn(iam_client, sts_client) -> str:
    """Busca o ARN da LabRole na conta AWS atual."""
    try:
        resposta = iam_client.get_role(RoleName="LabRole")
        return resposta["Role"]["Arn"]
    except ClientError:
        conta_id = sts_client.get_caller_identity()["Account"]
        return f"arn:aws:iam::{conta_id}:role/LabRole"


def garantir_tabela_dynamodb(dynamodb_client, nome_tabela: str, chave_particao: str, habilitar_stream: bool = False) -> dict:
    """Cria a tabela DynamoDB caso não exista e aguarda até ficar com status ACTIVE."""
    try:
        descricao = dynamodb_client.describe_table(TableName=nome_tabela)["Table"]
        print(f"  [OK] Tabela '{nome_tabela}' já existe (Status: {descricao['TableStatus']}).")

        if habilitar_stream:
            stream_spec = descricao.get("StreamSpecification", {})
            if not stream_spec.get("StreamEnabled"):
                print(f"  [..] Habilitando DynamoDB Stream em '{nome_tabela}'...")
                dynamodb_client.update_table(
                    TableName=nome_tabela,
                    StreamSpecification={
                        "StreamEnabled": True,
                        "StreamViewType": "NEW_AND_OLD_IMAGES",
                    },
                )
                time.sleep(5)
                descricao = dynamodb_client.describe_table(TableName=nome_tabela)["Table"]
        return descricao

    except ClientError as erro:
        if erro.response["Error"]["Code"] != "ResourceNotFoundException":
            raise

    print(f"  [..] Criando tabela DynamoDB '{nome_tabela}' (PK: {chave_particao})...")
    parametros = {
        "TableName": nome_tabela,
        "KeySchema": [{"AttributeName": chave_particao, "KeyType": "HASH"}],
        "AttributeDefinitions": [{"AttributeName": chave_particao, "AttributeType": "S"}],
        "BillingMode": "PAY_PER_REQUEST",
    }
    if habilitar_stream:
        parametros["StreamSpecification"] = {
            "StreamEnabled": True,
            "StreamViewType": "NEW_AND_OLD_IMAGES",
        }

    dynamodb_client.create_table(**parametros)
    waiter = dynamodb_client.get_waiter("table_exists")
    waiter.wait(TableName=nome_tabela)
    descricao = dynamodb_client.describe_table(TableName=nome_tabela)["Table"]
    print(f"  [OK] Tabela '{nome_tabela}' criada e ativa!")
    return descricao


def garantir_topico_sns(sns_client, nome_topico: str, email_assinante: str | None = None,
                        filtrar_por_email: bool = False) -> str:
    """
    Cria (ou recupera) o tópico SNS e opcionalmente cadastra um e-mail como assinante.
    Com 'filtrar_por_email', a assinatura recebe uma Filter Policy pelo atributo 'comprador_email'
    (publicado pelas Lambdas 1 e 3), e o assinante passa a receber apenas os avisos dos próprios pedidos.
    """
    resposta = sns_client.create_topic(Name=nome_topico)
    topic_arn = resposta["TopicArn"]
    print(f"  [OK] Tópico SNS pronto: {topic_arn}")

    if email_assinante:
        parametros = {
            "TopicArn": topic_arn,
            "Protocol": "email",
            "Endpoint": email_assinante.strip(),
        }
        if filtrar_por_email:
            parametros["Attributes"] = {
                "FilterPolicy": json.dumps({"comprador_email": [email_assinante.strip()]}),
            }
        sns_client.subscribe(**parametros)
        if filtrar_por_email:
            print(f"  [OK] Assinatura com Filter Policy: só recebe avisos de pedidos feitos com '{email_assinante}'.")
        print(
            f"  [!] Convite SNS enviado para '{email_assinante}'. "
            f"Abra sua caixa de entrada e clique em 'Confirm subscription'!"
        )
    return topic_arn


def empacotar_lambda_zip(caminho_arquivo: Path) -> bytes:
    """Compacta o arquivo lambda_function.py em memória no formato .zip exigido pela AWS."""
    buffer = io.BytesIO()
    with zipfile.ZipFile(buffer, mode="w", compression=zipfile.ZIP_DEFLATED) as arquivo_zip:
        arquivo_zip.write(caminho_arquivo, arcname="lambda_function.py")
    return buffer.getvalue()


def aguardar_lambda_pronta(lambda_client, nome_funcao: str) -> None:
    """Aguarda a função Lambda finalizar qualquer atualização pendente antes de prosseguir."""
    for _ in range(30):
        config = lambda_client.get_function_configuration(FunctionName=nome_funcao)
        estado = config.get("State", "Active")
        status_atualizacao = config.get("LastUpdateStatus", "Successful")
        if estado == "Active" and status_atualizacao in ("Successful", "InProgress") and status_atualizacao != "InProgress":
            return
        time.sleep(2)


def criar_ou_atualizar_lambda(lambda_client, cfg: dict, role_arn: str, topic_arn: str) -> str:
    """Cria a função Lambda ou atualiza seu código e variáveis de ambiente se ela já existir."""
    nome_funcao = cfg["nome_funcao"]
    pacote_zip = empacotar_lambda_zip(cfg["arquivo_origem"])
    variaveis_ambiente = {
        "Variables": {
            "TABLE_LIVROS": NOME_TABELA_LIVROS,
            "TABLE_PEDIDOS": NOME_TABELA_PEDIDOS,
            "SNS_TOPIC_NAME": NOME_TOPICO_SNS,
            "SNS_TOPIC_ARN": topic_arn,
        }
    }

    try:
        lambda_client.get_function(FunctionName=nome_funcao)
        print(f"  [..] Atualizando código da Lambda '{nome_funcao}'...")
        lambda_client.update_function_code(
            FunctionName=nome_funcao,
            ZipFile=pacote_zip,
        )
        aguardar_lambda_pronta(lambda_client, nome_funcao)
        resposta = lambda_client.update_function_configuration(
            FunctionName=nome_funcao,
            Role=role_arn,
            Handler="lambda_function.lambda_handler",
            Runtime="python3.12",
            Timeout=20,
            Environment=variaveis_ambiente,
        )
        aguardar_lambda_pronta(lambda_client, nome_funcao)
        print(f"  [OK] Lambda '{nome_funcao}' atualizada.")
        return resposta["FunctionArn"]

    except ClientError as erro:
        if erro.response["Error"]["Code"] != "ResourceNotFoundException":
            raise

    print(f"  [..] Criando função Lambda '{nome_funcao}'...")
    resposta = lambda_client.create_function(
        FunctionName=nome_funcao,
        Runtime="python3.12",
        Role=role_arn,
        Handler="lambda_function.lambda_handler",
        Code={"ZipFile": pacote_zip},
        Description=cfg["descricao"],
        Timeout=20,
        MemorySize=128,
        Environment=variaveis_ambiente,
    )
    aguardar_lambda_pronta(lambda_client, nome_funcao)
    print(f"  [OK] Lambda '{nome_funcao}' criada.")
    return resposta["FunctionArn"]


def garantir_function_url(lambda_client, nome_funcao: str) -> str:
    """Configura a Function URL pública (AuthType=NONE) e aplica a permissão de invocação pública."""
    cors_config = {
        "AllowCredentials": False,
        "AllowHeaders": ["*"],
        "AllowMethods": ["*"],
        "AllowOrigins": ["*"],
    }

    try:
        config_url = lambda_client.get_function_url_config(FunctionName=nome_funcao)
        function_url = config_url["FunctionUrl"]
    except ClientError as erro:
        if erro.response["Error"]["Code"] != "ResourceNotFoundException":
            raise
        config_url = lambda_client.create_function_url_config(
            FunctionName=nome_funcao,
            AuthType="NONE",
            Cors=cors_config,
        )
        function_url = config_url["FunctionUrl"]

    # Garante a permissão pública para invocação via navegador / HTTP cliente
    try:
        lambda_client.add_permission(
            FunctionName=nome_funcao,
            StatementId="FunctionURLAllowPublicAccess",
            Action="lambda:InvokeFunctionUrl",
            Principal="*",
            FunctionUrlAuthType="NONE",
        )
    except ClientError as erro:
        if erro.response["Error"]["Code"] != "ResourceConflictException":
            raise

    try:
        lambda_client.add_permission(
            FunctionName=nome_funcao,
            StatementId="FunctionURLAllowPublicInvoke",
            Action="lambda:InvokeFunction",
            Principal="*",
        )
    except ClientError as erro:
        if erro.response["Error"]["Code"] != "ResourceConflictException":
            raise

    print(f"  [OK] Function URL ({nome_funcao}): {function_url}")
    return function_url


def garantir_fila_falhas(sqs_client, nome_fila: str) -> str | None:
    """
    Cria (ou recupera) a fila SQS que recebe os registros do Stream que falharam em todas as tentativas.
    Retorna o ARN da fila, ou None se não for possível criá-la (o gatilho segue funcionando sem ela).
    """
    try:
        url_fila = sqs_client.create_queue(
            QueueName=nome_fila,
            Attributes={"MessageRetentionPeriod": str(14 * 24 * 3600)},
        )["QueueUrl"]
        arn_fila = sqs_client.get_queue_attributes(
            QueueUrl=url_fila, AttributeNames=["QueueArn"]
        )["Attributes"]["QueueArn"]
        print(f"  [OK] Fila de falhas (DLQ) pronta: {arn_fila}")
        return arn_fila
    except ClientError as erro:
        print(f"  [AVISO] Não foi possível criar a fila de falhas '{nome_fila}': {erro}")
        return None


def garantir_trigger_stream(lambda_client, nome_funcao: str, stream_arn: str, arn_fila_falhas: str | None) -> None:
    """
    Conecta o DynamoDB Stream da tabela SeboLivros como gatilho da Lambda Notificadora.
    - ReportBatchItemFailures: a Lambda informa quais registros falharam e só eles (e os seguintes) são reenviados;
    - MaximumRetryAttempts: limita as tentativas para um registro com erro não travar o Stream;
    - OnFailure: registros que esgotarem as tentativas vão para a fila SQS de falhas.
    """
    configuracao_erros = {
        "FunctionResponseTypes": ["ReportBatchItemFailures"],
        "MaximumRetryAttempts": MAXIMO_TENTATIVAS_STREAM,
        "BisectBatchOnFunctionError": True,
    }
    if arn_fila_falhas:
        configuracao_erros["DestinationConfig"] = {"OnFailure": {"Destination": arn_fila_falhas}}

    mapeamentos = lambda_client.list_event_source_mappings(
        FunctionName=nome_funcao,
        EventSourceArn=stream_arn,
    ).get("EventSourceMappings", [])

    if mapeamentos:
        uuid_mapeamento = mapeamentos[0]["UUID"]
        estado = mapeamentos[0].get("State", "")
        print(f"  [OK] Trigger DynamoDB Stream já conectado (UUID: {uuid_mapeamento}, Estado: {estado}).")
        lambda_client.update_event_source_mapping(UUID=uuid_mapeamento, Enabled=True, **configuracao_erros)
        print("  [OK] Configuração de reprocessamento e fila de falhas atualizada.")
        return

    print(f"  [..] Conectando DynamoDB Stream à Lambda '{nome_funcao}'...")
    lambda_client.create_event_source_mapping(
        EventSourceArn=stream_arn,
        FunctionName=nome_funcao,
        Enabled=True,
        BatchSize=5,
        StartingPosition="LATEST",
        **configuracao_erros,
    )
    print(f"  [OK] Gatilho DynamoDB Stream ativado para '{nome_funcao}'!")


def salvar_arquivo_env(urls_geradas: dict) -> Path:
    """Grava as Function URLs geradas no arquivo .env na raiz do projeto."""
    caminho_env = RAIZ_PROJETO / ".env"
    linhas = [
        "# Gerado automaticamente por scripts/setup_aws.py",
        f"LAMBDA_COMPRADOR_URL={urls_geradas.get('LAMBDA_COMPRADOR_URL', '')}",
        f"LAMBDA_COLABORADOR_URL={urls_geradas.get('LAMBDA_COLABORADOR_URL', '')}",
        "",
    ]
    caminho_env.write_text("\n".join(linhas), encoding="utf-8")
    return caminho_env


def main():
    parser = argparse.ArgumentParser(description="Provisiona a infraestrutura do Sebo Serverless na AWS.")
    parser.add_argument(
        "--email",
        help="E-mail opcional para já cadastrar como assinante no tópico Amazon SNS.",
        default=None,
    )
    parser.add_argument(
        "--filtrar-por-email",
        action="store_true",
        help=(
            "Aplica uma Filter Policy na assinatura do --email, para que ele receba apenas os avisos "
            "dos pedidos feitos com esse e-mail (sem a opção, recebe os avisos de todos os pedidos)."
        ),
    )
    args = parser.parse_args()
    if args.filtrar_por_email and not args.email:
        parser.error("--filtrar-por-email exige --email.")

    print("=" * 72)
    print("  PROVISIONAMENTO AUTOMATIZADO - SEBO SERVERLESS NA AWS (us-east-1)")
    print("=" * 72)

    sessao = boto3.Session(region_name=REGIAO_AWS)
    sts_client = sessao.client("sts")
    iam_client = sessao.client("iam")
    dynamodb_client = sessao.client("dynamodb")
    sns_client = sessao.client("sns")
    lambda_client = sessao.client("lambda")
    sqs_client = sessao.client("sqs")

    identidade = sts_client.get_caller_identity()
    role_arn = obter_lab_role_arn(iam_client, sts_client)
    print(f"\n1. Conta AWS identificada: {identidade['Account']}")
    print(f"   Role utilizada: {role_arn}")

    print("\n2. Verificando/Criando tabelas no Amazon DynamoDB...")
    tabela_livros_desc = garantir_tabela_dynamodb(
        dynamodb_client, NOME_TABELA_LIVROS, chave_particao="livro_id", habilitar_stream=True
    )
    garantir_tabela_dynamodb(
        dynamodb_client, NOME_TABELA_PEDIDOS, chave_particao="pedido_id", habilitar_stream=False
    )
    stream_arn = tabela_livros_desc.get("LatestStreamArn")

    print("\n3. Verificando/Criando tópico no Amazon SNS...")
    topic_arn = garantir_topico_sns(
        sns_client, NOME_TOPICO_SNS, email_assinante=args.email, filtrar_por_email=args.filtrar_por_email
    )

    print("\n4. Implantando as 3 funções AWS Lambda...")
    urls_geradas = {}
    for cfg in CONFIG_LAMBDAS:
        criar_ou_atualizar_lambda(lambda_client, cfg, role_arn=role_arn, topic_arn=topic_arn)
        if cfg["criar_url"]:
            url = garantir_function_url(lambda_client, cfg["nome_funcao"])
            urls_geradas[cfg["env_var_url"]] = url

    print("\n5. Configurando gatilho do DynamoDB Stream na Lambda 3...")
    if stream_arn:
        arn_fila_falhas = garantir_fila_falhas(sqs_client, NOME_FILA_FALHAS)
        garantir_trigger_stream(lambda_client, "sebo-notificador-estoque", stream_arn, arn_fila_falhas)
    else:
        print("  [AVISO] LatestStreamArn não encontrado na tabela SeboLivros.")

    caminho_env = salvar_arquivo_env(urls_geradas)

    print("\n" + "=" * 72)
    print("  INFRAESTRUTURA PRONTA COM SUCESSO!")
    print("=" * 72)
    print(f"  LAMBDA_COMPRADOR_URL   = {urls_geradas.get('LAMBDA_COMPRADOR_URL')}")
    print(f"  LAMBDA_COLABORADOR_URL = {urls_geradas.get('LAMBDA_COLABORADOR_URL')}")
    print(f"\n  As URLs acima já foram salvas automaticamente em: {caminho_env}")
    print("=" * 72)


if __name__ == "__main__":
    main()
