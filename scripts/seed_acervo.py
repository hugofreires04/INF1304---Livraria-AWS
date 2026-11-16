"""
Carga Inicial de Livros no Sebo (seed_acervo.py)

Popula o catálogo inicial do sebo com livros clássicos brasileiros e estrangeiros:
- Alguns livros já começam com estoque disponível (quantidade > 0) para testar o Fluxo 1 (Compra Imediata);
- Outros livros começam esgotados (quantidade == 0) para testar o Fluxo 2 (Fila de Espera) e o Fluxo 3 (Reposição).

Pode operar de duas formas:
1. Via Function URL da Lambda do Colaborador (lendo LAMBDA_COLABORADOR_URL do arquivo .env);
2. Direto no DynamoDB via boto3 caso a flag --direto-dynamodb seja informada.

Uso:
  python scripts/seed_acervo.py
  python scripts/seed_acervo.py --direto-dynamodb
"""

import argparse
import os
from datetime import datetime, timezone
from pathlib import Path

import requests
from dotenv import load_dotenv

RAIZ_PROJETO = Path(__file__).resolve().parent.parent

LIVROS_INICIAIS = [
    {
        "livro_id": "dom-casmurro",
        "titulo": "Dom Casmurro",
        "autor": "Machado de Assis",
        "preco": "25.00",
        "quantidade_adicional": 2,
    },
    {
        "livro_id": "memorias-postumas-de-bras-cubas",
        "titulo": "Memórias Póstumas de Brás Cubas",
        "autor": "Machado de Assis",
        "preco": "28.00",
        "quantidade_adicional": 1,
    },
    {
        "livro_id": "a-hora-da-estrela",
        "titulo": "A Hora da Estrela",
        "autor": "Clarice Lispector",
        "preco": "22.00",
        "quantidade_adicional": 3,
    },
    {
        "livro_id": "vidas-secas",
        "titulo": "Vidas Secas",
        "autor": "Graciliano Ramos",
        "preco": "30.00",
        "quantidade_adicional": 0,  # Começa esgotado para demonstrar a Fila de Espera
    },
    {
        "livro_id": "capitaes-da-areia",
        "titulo": "Capitães da Areia",
        "autor": "Jorge Amado",
        "preco": "27.50",
        "quantidade_adicional": 0,  # Começa esgotado para demonstrar a Fila de Espera
    },
]


def popular_via_lambda(url_colaborador: str) -> None:
    """Envia cada livro inicial para a Function URL da Lambda do Colaborador."""
    print(f"Populando acervo via Lambda do Colaborador: {url_colaborador}\n")
    for item in LIVROS_INICIAIS:
        payload = {
            "acao": "cadastrar_ou_repor_livro",
            "livro_id": item["livro_id"],
            "titulo": item["titulo"],
            "autor": item["autor"],
            "preco": item["preco"],
            "quantidade_adicional": item["quantidade_adicional"],
        }
        resposta = requests.post(url_colaborador, json=payload, timeout=20)
        dados = resposta.json()
        status = "OK" if resposta.ok and dados.get("sucesso") else "ERRO"
        print(
            f"  [{status}] {item['titulo']} ({item['autor']}) "
            f"-> +{item['quantidade_adicional']} un. | R$ {item['preco']}"
        )


def popular_direto_dynamodb() -> None:
    """Insere os livros iniciais diretamente na tabela SeboLivros via boto3."""
    import boto3

    regiao = os.environ.get("AWS_REGION", "us-east-1")
    nome_tabela = os.environ.get("TABLE_LIVROS", "SeboLivros")
    tabela = boto3.resource("dynamodb", region_name=regiao).Table(nome_tabela)
    agora = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")

    print(f"Populando acervo diretamente na tabela DynamoDB '{nome_tabela}'...\n")
    for item in LIVROS_INICIAIS:
        registro = {
            "livro_id": item["livro_id"],
            "titulo": item["titulo"],
            "autor": item["autor"],
            "preco": item["preco"],
            "quantidade": item["quantidade_adicional"],
            "total_vendidos": 0,
            "atualizado_em": agora,
        }
        tabela.put_item(Item=registro)
        print(
            f"  [OK] {item['titulo']} ({item['autor']}) "
            f"-> estoque fixado em {item['quantidade_adicional']} un."
        )


def main():
    parser = argparse.ArgumentParser(description="Popula o acervo inicial do Sebo na AWS.")
    parser.add_argument(
        "--direto-dynamodb",
        action="store_true",
        help="Grava diretamente na tabela SeboLivros via boto3 (reseta as quantidades para o valor inicial).",
    )
    args = parser.parse_args()

    if args.direto_dynamodb:
        popular_direto_dynamodb()
        return

    load_dotenv(RAIZ_PROJETO / ".env")
    load_dotenv()
    url_colaborador = os.environ.get("LAMBDA_COLABORADOR_URL", "").strip()

    if not url_colaborador or "yyyy" in url_colaborador:
        print(
            "[ERRO] Variável LAMBDA_COLABORADOR_URL não configurada no arquivo .env.\n"
            "Execute primeiro 'python scripts/setup_aws.py' ou use '--direto-dynamodb'."
        )
        return

    popular_via_lambda(url_colaborador)


if __name__ == "__main__":
    main()
