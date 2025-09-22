import pandas as pd

# caminho relativo para o arquivo
caminho = "/Users/dudabosquerolli/Dev/livros/data/raw/minha_biblioteca.csv"

# le o CSV
df = pd.read_csv(caminho)

# mostra informacoes basicas
print("Primeiras linhas:")
print(df.head())

print("\nResumo:")
print(df.info())