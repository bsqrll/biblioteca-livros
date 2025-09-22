from pathlib import Path

BASE = Path(__file__).resolve().parents[2]  # sobe até a raiz do projeto
raw = BASE / "data" / "raw" / "minha_biblioteca.csv"

print("BASE:", BASE)
print("Arquivo existe?", raw.exists(), "->", raw)

import pandas as pd

df = pd.read_csv(raw)
print("\nShape:", df.shape)
print("\nColunas:", list(df.columns))
print("\nHead:")
print(df.head(5))

# trocar vírgula por ponto e converter pra float (mantém NaN onde vazio)
df["nota_minha"] = pd.to_numeric(
    df["nota_minha"].astype(str).str.replace(",", ".", regex=False),
    errors="coerce"
)
print("\nNotas (amostra):")
print(df["nota_minha"].head(10))
print("\nQuantas notas vazias:", df["nota_minha"].isna().sum())

# remover hifens e espaços
df["isbn_clean"] = df["isbn"].astype(str).str.replace("-", "", regex=False).str.strip()

print("\nISBNs limpos (amostra):")
print(df[["isbn", "isbn_clean"]].head(10))

# checar se todos têm 13 dígitos
df["isbn_len_ok"] = df["isbn_clean"].str.len().eq(13)

print("\nISBNs com tamanho errado:")
print(df.loc[~df["isbn_len_ok"], ["titulo", "isbn", "isbn_clean"]])

# validação de ISBN-13 (dígito verificador)
def is_isbn13_ok(s: str) -> bool:
    if len(s) != 13 or not s.isdigit():
        return False
    soma = sum((int(d) * (1 if i % 2 == 0 else 3)) for i, d in enumerate(s[:-1]))
    dv = (10 - (soma % 10)) % 10
    return dv == int(s[-1])

df["isbn_dv_ok"] = df["isbn_clean"].apply(is_isbn13_ok)

print("\nISBN com dígito verificador inválido (entre os que têm 13 dígitos):")
print(df[(df["isbn_len_ok"]) & (~df["isbn_dv_ok"])][["titulo","autor","isbn","isbn_clean"]])

dups = df[df["isbn_clean"].duplicated(keep=False)].sort_values("isbn_clean")
print("\nDuplicados por ISBN:")
print(dups[["isbn_clean","titulo","autor"]])

from pathlib import Path

out = BASE / "data" / "processed" / "minha_biblioteca_clean.csv"

# mantém as colunas base + isbn_clean
df_out = df.copy()
df_out["isbn"] = df_out["isbn_clean"]
df_out = df_out[["titulo","autor","isbn","nota_minha","status"]]

df_out.to_csv(out, index=False)
print(f"\n✅ Salvo: {out}")