"""
Interface de terminal do Colaborador do sebo.

Consome a Function URL da Lambda sebo-colaborador-service para:
- Ver o painel de estoque, vendas e fila de espera;
- Cadastrar novos livros ou repor exemplares (a fila de espera é atendida
  automaticamente pelo notificador acionado via DynamoDB Stream).

A URL é lida da variável LAMBDA_COLABORADOR_URL no arquivo .env (raiz do projeto)
ou solicitada no terminal caso não esteja configurada.

Uso:  python interfaces/cli_colaborador.py
"""

import os
import sys
from pathlib import Path

try:
    import requests
    from dotenv import load_dotenv
except ImportError:
    print("Dependências ausentes. Instale com:  pip install -r requirements.txt")
    sys.exit(1)

RAIZ_PROJETO = Path(__file__).resolve().parent.parent
TIMEOUT_SEGUNDOS = 30

STATUS_LEGIVEL = {
    "DISPONIVEL_NOTIFICADO": "Separado (estoque imediato)",
    "CHEGOU_NOTIFICADO": "Chegou - cliente avisado",
    "AGUARDANDO_ESTOQUE": "Na fila de espera",
}


def carregar_url_lambda() -> str:
    """Lê LAMBDA_COLABORADOR_URL do .env; se não existir, pede ao usuário no terminal."""
    load_dotenv(RAIZ_PROJETO / ".env")
    load_dotenv()  # também aceita um .env no diretório atual
    url = os.environ.get("LAMBDA_COLABORADOR_URL", "").strip()
    while not url:
        url = input("Cole a Function URL da Lambda do Colaborador (LAMBDA_COLABORADOR_URL): ").strip()
    return url


def chamar_lambda(url: str, payload: dict) -> dict | None:
    """Envia o payload JSON via POST e retorna a resposta; exibe o erro e retorna None em caso de falha."""
    try:
        resposta = requests.post(url, json=payload, timeout=TIMEOUT_SEGUNDOS)
    except requests.RequestException as erro:
        print(f"\n[ERRO] Falha de conexão com a Lambda: {erro}")
        return None

    try:
        dados = resposta.json()
    except ValueError:
        print(f"\n[ERRO] Resposta inválida da Lambda (HTTP {resposta.status_code}): {resposta.text[:200]}")
        return None

    if not resposta.ok or dados.get("sucesso") is False:
        print(f"\n[ERRO] {dados.get('mensagem', f'HTTP {resposta.status_code}')}")
        return None
    return dados


def formatar_preco(preco) -> str:
    """Formata o preço em reais; retorna '-' quando não informado."""
    try:
        valor = float(preco)
    except (TypeError, ValueError):
        return "-"
    return f"R$ {valor:.2f}".replace(".", ",") if valor > 0 else "-"


def cortar(texto, largura: int) -> str:
    """Corta o texto para caber na coluna da tabela."""
    texto = str(texto or "")
    return texto if len(texto) <= largura else texto[: largura - 1] + "…"


def obter_painel(url: str) -> dict | None:
    """Busca o painel consolidado (resumo, livros e pedidos) na Lambda do Colaborador."""
    return chamar_lambda(url, {"acao": "painel_estoque"})


def mostrar_painel(url: str) -> None:
    """Exibe o resumo e a tabela de estoque por livro (com clientes aguardando)."""
    dados = obter_painel(url)
    if dados is None:
        return

    resumo = dados.get("resumo", {})
    print("\n--- Resumo do sebo ---")
    print(f"  Títulos no acervo ........... {resumo.get('total_titulos', 0)}")
    print(f"  Exemplares disponíveis ...... {resumo.get('total_exemplares_disponiveis', 0)}")
    print(f"  Vendas / reservas ........... {resumo.get('total_vendas_reservadas', 0)}")
    print(f"  Clientes na fila de espera .. {resumo.get('total_fila_espera', 0)}")

    livros = dados.get("livros", [])
    if not livros:
        print("\nNenhum livro cadastrado ainda.")
        return

    print(f"\n{'CÓDIGO':<24} {'TÍTULO':<30} {'AUTOR':<20} {'PREÇO':>10} {'ESTOQUE':>8} {'VENDIDOS':>9} {'AGUARDANDO':>11}")
    print("-" * 118)
    for livro in livros:
        print(
            f"{cortar(livro.get('livro_id'), 24):<24} "
            f"{cortar(livro.get('titulo'), 30):<30} "
            f"{cortar(livro.get('autor'), 20):<20} "
            f"{formatar_preco(livro.get('preco')):>10} "
            f"{livro.get('quantidade', 0):>8} "
            f"{livro.get('total_vendidos', 0):>9} "
            f"{livro.get('clientes_aguardando', 0):>11}"
        )


def mostrar_pedidos(url: str, somente_fila: bool) -> None:
    """Exibe os pedidos registrados (todos ou apenas os que estão na fila de espera)."""
    dados = obter_painel(url)
    if dados is None:
        return

    pedidos = dados.get("pedidos", [])
    if somente_fila:
        pedidos = sorted(
            (p for p in pedidos if p.get("status") == "AGUARDANDO_ESTOQUE"),
            key=lambda p: p.get("criado_em", ""),
        )

    if not pedidos:
        print("\nNenhum pedido " + ("na fila de espera." if somente_fila else "registrado."))
        return

    print(f"\n{'PEDIDO':<14} {'LIVRO':<26} {'COMPRADOR':<28} {'STATUS':<28} {'CRIADO EM':<22} AVISADO EM")
    print("-" * 140)
    for p in pedidos:
        comprador = f"{p.get('comprador_nome', '')} <{p.get('comprador_email', '')}>"
        print(
            f"{p.get('pedido_id', ''):<14} "
            f"{cortar(p.get('titulo'), 26):<26} "
            f"{cortar(comprador, 28):<28} "
            f"{STATUS_LEGIVEL.get(p.get('status'), p.get('status', '')):<28} "
            f"{p.get('criado_em') or '-':<22} "
            f"{p.get('notificado_em') or '-'}"
        )
    print(f"\nTotal: {len(pedidos)} pedido(s).")


def cadastrar_ou_repor(url: str) -> None:
    """Registra a chegada de exemplares de um livro (novo ou já existente no acervo)."""
    print("\n--- Cadastrar ou repor livro ---")
    titulo = input("Título: ").strip()
    if not titulo:
        print("\n[ERRO] O título é obrigatório.")
        return
    autor = input("Autor: ").strip()
    preco = input("Preço (ex: 25.00): ").strip().replace(",", ".")
    try:
        quantidade = int(input("Exemplares chegando [1]: ").strip() or "1")
    except ValueError:
        print("\n[ERRO] A quantidade deve ser um número inteiro.")
        return

    payload = {
        "acao": "cadastrar_ou_repor_livro",
        "titulo": titulo,
        "autor": autor,
        "preco": preco,
        "quantidade_adicional": quantidade,
    }
    dados = chamar_lambda(url, payload)
    if dados is None:
        return

    livro = dados.get("livro", {})
    print(f"\n{dados.get('mensagem', '')}")
    print(f"   {livro.get('titulo', titulo)} ({livro.get('livro_id', '-')}): {livro.get('quantidade', '-')} exemplar(es) no estoque.")
    print("   Dica: use a opção 3 em alguns segundos para ver a fila de espera sendo atendida.")


def main() -> None:
    """Loop principal do menu interativo do colaborador."""
    url = carregar_url_lambda()

    opcoes = {
        "1": ("Painel de estoque e resumo", lambda: mostrar_painel(url)),
        "2": ("Cadastrar ou repor livro", lambda: cadastrar_ou_repor(url)),
        "3": ("Ver fila de espera", lambda: mostrar_pedidos(url, somente_fila=True)),
        "4": ("Ver todos os pedidos (vendas e fila)", lambda: mostrar_pedidos(url, somente_fila=False)),
    }

    print("\n==============================")
    print("  Sebo AWS - Colaborador")
    print("==============================")

    while True:
        print()
        for chave, (descricao, _) in opcoes.items():
            print(f"  {chave}) {descricao}")
        print("  0) Sair")

        escolha = input("\nOpção: ").strip()
        if escolha == "0":
            print("Até logo!")
            break
        if escolha in opcoes:
            opcoes[escolha][1]()
        else:
            print("Opção inválida.")


if __name__ == "__main__":
    try:
        main()
    except (KeyboardInterrupt, EOFError):
        print("\nAté logo!")
