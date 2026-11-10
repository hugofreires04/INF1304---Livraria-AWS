"""
Interface de terminal do Comprador.

Consome a Function URL da Lambda sebo-comprador-service para:
- Listar o acervo do sebo;
- Solicitar um livro (reserva imediata ou fila de espera);
- Consultar os próprios pedidos pelo e-mail.

A URL é lida da variável LAMBDA_COMPRADOR_URL no arquivo .env (raiz do projeto)
ou solicitada no terminal caso não esteja configurada.

Uso:  python interfaces/cli_comprador.py
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
    """Lê LAMBDA_COMPRADOR_URL do .env; se não existir, pede ao usuário no terminal."""
    load_dotenv(RAIZ_PROJETO / ".env")
    load_dotenv()  # também aceita um .env no diretório atual
    url = os.environ.get("LAMBDA_COMPRADOR_URL", "").strip()
    while not url:
        url = input("Cole a Function URL da Lambda do Comprador (LAMBDA_COMPRADOR_URL): ").strip()
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


def listar_acervo(url: str) -> None:
    """Mostra o catálogo do sebo com a disponibilidade de cada livro."""
    dados = chamar_lambda(url, {"acao": "listar_livros"})
    if dados is None:
        return

    livros = dados.get("livros", [])
    if not livros:
        print("\nNenhum livro cadastrado no acervo.")
        return

    print(f"\n{'CÓDIGO':<24} {'TÍTULO':<30} {'AUTOR':<22} {'PREÇO':>10}  DISPONIBILIDADE")
    print("-" * 110)
    for livro in livros:
        qtd = int(livro.get("quantidade", 0) or 0)
        disponibilidade = f"{qtd} disponível(is)" if qtd > 0 else "Esgotado - aceita encomenda"
        print(
            f"{cortar(livro.get('livro_id'), 24):<24} "
            f"{cortar(livro.get('titulo'), 30):<30} "
            f"{cortar(livro.get('autor'), 22):<22} "
            f"{formatar_preco(livro.get('preco')):>10}  "
            f"{disponibilidade}"
        )
    print(f"\nTotal: {dados.get('total', len(livros))} título(s).")


def solicitar_livro(url: str, sessao: dict) -> None:
    """Pede os dados do comprador e do livro e registra a solicitação."""
    print("\n--- Solicitar livro ---")
    nome = input(f"Seu nome [{sessao.get('nome', '')}]: ").strip() or sessao.get("nome", "")
    email = input(f"Seu e-mail [{sessao.get('email', '')}]: ").strip() or sessao.get("email", "")
    titulo = input("Título do livro: ").strip()
    livro_id = input("Código do livro (opcional, Enter para gerar pelo título): ").strip()

    if not nome or not email or not (titulo or livro_id):
        print("\n[ERRO] Nome, e-mail e título (ou código) são obrigatórios.")
        return

    sessao["nome"], sessao["email"] = nome, email

    payload = {
        "acao": "solicitar_livro",
        "titulo": titulo,
        "comprador_nome": nome,
        "comprador_email": email,
    }
    if livro_id:
        payload["livro_id"] = livro_id

    dados = chamar_lambda(url, payload)
    if dados is None:
        return

    pedido = dados.get("pedido", {})
    if dados.get("disponivel_imediato"):
        print("\nLIVRO DISPONÍVEL!")
        if not dados.get("email_enviado"):
            print("   (Atenção: o e-mail via SNS não pôde ser enviado.)")
    else:
        print("\nLIVRO SEM ESTOQUE - você entrou na fila de espera.")
    print(f"   {dados.get('mensagem', '')}")
    print(f"   Pedido: {pedido.get('pedido_id', '-')} | Status: {STATUS_LEGIVEL.get(pedido.get('status'), pedido.get('status'))}")


def consultar_meus_pedidos(url: str, sessao: dict) -> None:
    """Lista os pedidos associados ao e-mail informado."""
    email = input(f"\nSeu e-mail [{sessao.get('email', '')}]: ").strip() or sessao.get("email", "")
    if not email:
        print("\n[ERRO] Informe um e-mail.")
        return
    sessao["email"] = email

    dados = chamar_lambda(url, {"acao": "meus_pedidos", "comprador_email": email})
    if dados is None:
        return

    pedidos = dados.get("pedidos", [])
    if not pedidos:
        print("\nNenhum pedido encontrado para este e-mail.")
        return

    print(f"\n{'PEDIDO':<14} {'LIVRO':<30} {'STATUS':<30} {'CRIADO EM':<22} AVISADO EM")
    print("-" * 120)
    for p in pedidos:
        print(
            f"{p.get('pedido_id', ''):<14} "
            f"{cortar(p.get('titulo'), 30):<30} "
            f"{STATUS_LEGIVEL.get(p.get('status'), p.get('status', '')):<30} "
            f"{p.get('criado_em') or '-':<22} "
            f"{p.get('notificado_em') or '-'}"
        )
    print(f"\nTotal: {dados.get('total', len(pedidos))} pedido(s).")


def main() -> None:
    """Loop principal do menu interativo do comprador."""
    url = carregar_url_lambda()
    sessao = {}

    opcoes = {
        "1": ("Listar acervo do sebo", lambda: listar_acervo(url)),
        "2": ("Solicitar um livro", lambda: solicitar_livro(url, sessao)),
        "3": ("Consultar meus pedidos", lambda: consultar_meus_pedidos(url, sessao)),
    }

    print("\n==============================")
    print("  Sebo AWS - Comprador")
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
