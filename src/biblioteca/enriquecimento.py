from pathlib import Path
import time, json, hashlib
import requests
import pandas as pd
from unidecode import unidecode

# === ETAPA 1 — localizar e ler o CSV clean ===
BASE = Path(__file__).resolve().parents[2]  # raiz do projeto
clean = BASE / "data" / "processed" / "minha_biblioteca_clean.csv"

print("BASE:", BASE)
print("Existe o CSV clean?", clean.exists(), "-->", clean)

df = pd.read_csv(clean)
print("Shape:", df.shape)
print(df.head(3))

# === Configs gerais ===
headers = {"User-Agent": "meu-projeto-livros/0.1"}

# pastas + arquivos de saída
interim_dir = BASE / "data" / "interim"
interim_dir.mkdir(parents=True, exist_ok=True)
cache_dir = interim_dir / "cache_search_titulo_autor"
cache_dir.mkdir(parents=True, exist_ok=True)

out_final = BASE / "data" / "processed" / "biblioteca_enriquecida.csv"
out_checkpoint = interim_dir / "biblioteca_enriquecida_checkpoint.csv"
out_falhas = interim_dir / "enriquecimento_falhas_ta.csv"

# === helpers de normalização e scoring ===
def _norm(s: str) -> str:
    return unidecode((s or "").strip().lower())

def _sim_score(title_query: str, author_query: str, doc: dict):
    """Score simples: bate título e autor; usa edition_count como desempate."""
    t_q = _norm(title_query)
    a_q = _norm(author_query)
    t_d = _norm(doc.get("title") or "")
    a_ds = [_norm(a) for a in (doc.get("author_name") or [])]

    title_hit = int(t_q in t_d or t_d in t_q)
    author_hit = int(any(a_q in a for a in a_ds))
    return (title_hit + author_hit, doc.get("edition_count", 0))

def _cache_key(title: str, author: str) -> str:
    raw = f"{_norm(title)}|{_norm(author)}".encode()
    return hashlib.sha1(raw).hexdigest()

def _search_title_author(title: str, author: str) -> dict:
    """
    Chama /search.json?title=&author= com cache em disco.
    Retorna o JSON bruto do endpoint (ou {'status':code,'docs':[]}).
    """
    key = _cache_key(title, author)
    path = cache_dir / f"{key}.json"
    if path.exists():
        try:
            return json.loads(path.read_text(encoding="utf-8"))
        except Exception:
            pass  # se o cache corromper, ignora e refaz

    url = "https://openlibrary.org/search.json"
    params = {"title": title, "author": author, "limit": 10}
    r = requests.get(url, params=params, headers=headers, timeout=20)
    if r.status_code != 200:
        data = {"status": r.status_code, "docs": []}
    else:
        data = r.json()
        data["status"] = 200  # normaliza presença do status

    # grava cache
    try:
        path.write_text(json.dumps(data), encoding="utf-8")
    except Exception:
        pass

    return data

def _extract_from_search_doc(doc: dict) -> dict:
    def join_list(x):
        if isinstance(x, list):
            return ", ".join(map(str, x))
        return x

    return {
        "titulo_ol": doc.get("title"),
        "autores_ol": join_list(doc.get("author_name")),
        "paginas": doc.get("number_of_pages_median"),
        "ano_pub": doc.get("first_publish_year"),
        "editora": join_list(doc.get("publisher")),
        "categorias": join_list(doc.get("subject")),
        "obra_key": doc.get("key"),
    }

def enrich_row_by_title_author(titulo: str, autor: str, isbn13: str) -> dict:
    data = _search_title_author(titulo, autor)
    status = data.get("status", 200)
    docs = data.get("docs") or []

    if docs:
        best = sorted(docs, key=lambda d: _sim_score(titulo, autor, d), reverse=True)[0]
        fields = _extract_from_search_doc(best)
        fonte = "search_ta"
    else:
        fields = {k: None for k in
                  ["titulo_ol", "autores_ol", "paginas", "ano_pub", "editora", "categorias", "obra_key"]}
        fonte = "none"

    return {
        "isbn": isbn13,
        "titulo_planilha": titulo,
        "autor_planilha": autor,
        **fields,
        "fonte": fonte,
        "http_status": status,
    }

# === LOOP GERAL — só TÍTULO+AUTOR com cache + checkpoint ===
print("\n[RUN — título+autor] começando…")

rows = []
processados = set()

# se quiser retomar de checkpoint, descomente:
# if out_checkpoint.exists():
#     prev = pd.read_csv(out_checkpoint)
#     rows = prev.to_dict(orient="records")
#     processados = set(str(x) for x in prev["isbn"].astype(str).tolist())

start = time.time()

for i, row in enumerate(df.itertuples(index=False), start=1):
    titulo = str(getattr(row, "titulo"))
    autor  = str(getattr(row, "autor"))
    isbn13 = str(getattr(row, "isbn"))

    if isbn13 in processados:
        continue

    try:
        enriched = enrich_row_by_title_author(titulo, autor, isbn13)
        rows.append(enriched)
        print(f"[{i}/{len(df)}] {isbn13} -> {enriched['fonte']} | páginas={enriched['paginas']} | ano={enriched['ano_pub']}")
    except Exception as e:
        print(f"[{i}/{len(df)}] ERRO {isbn13}: {e}")
        rows.append({
            "isbn": isbn13,
            "titulo_planilha": titulo,
            "autor_planilha": autor,
            "titulo_ol": None,
            "autores_ol": None,
            "paginas": None,
            "ano_pub": None,
            "editora": None,
            "categorias": None,
            "obra_key": None,
            "fonte": f"error:{type(e).__name__}",
            "http_status": None,
        })

    # checkpoint a cada 20 linhas
    if i % 20 == 0:
        pd.DataFrame(rows).to_csv(out_checkpoint, index=False)
        print(f"💾 checkpoint salvo em {out_checkpoint}")

    # rate limit gentil
    time.sleep(1.3)

elapsed = time.time() - start
print(f"\nConcluído em {elapsed:.1f}s")

# salva final
pd.DataFrame(rows).to_csv(out_final, index=False)
print(f"✅ CSV final salvo em: {out_final}")

# relatório de falhas
falhas = [r for r in rows if (r["fonte"] == "none") or (isinstance(r["fonte"], str) and r["fonte"].startswith("error:"))]
if falhas:
    pd.DataFrame(falhas).to_csv(out_falhas, index=False)
    print(f"⚠️ Falhas salvas em: {out_falhas}")
else:
    print("Sem falhas 🎉")