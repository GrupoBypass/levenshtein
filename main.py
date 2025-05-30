import pandas as pd
import boto3
from io import StringIO


def put_file_to_s3(file_name, df: pd.DataFrame()):
    s3 = boto3.client('s3',
                      aws_access_key_id='',
                      aws_secret_access_key='',
                      aws_session_token='')

    bucket_name = 'client-bypass'
    file_key = f'data/{file_name}.csv'

    csv_buffer = StringIO()
    df.to_csv(csv_buffer, index=False)

    s3.put_object(Bucket=bucket_name, Key=file_key, Body=csv_buffer.getvalue())


def import_tweets() -> list:
    df = pd.read_csv('tweets.csv')

    return df['text'].str.split()


def levenshtein_distance(s1, s2):

    rows = len(s1) + 1
    cols = len(s2) + 1
    dist = [[0 for _ in range(cols)] for _ in range(rows)]

    for i in range(1, rows):
        dist[i][0] = i
    for j in range(1, cols):
        dist[0][j] = j

    for i in range(1, rows):
        for j in range(1, cols):
            cost = 0 if s1[i-1] == s2[j-1] else 1

            dist[i][j] = min(
                dist[i-1][j] + 1,      # Deletion
                dist[i][j-1] + 1,      # Insertion
                dist[i-1][j-1] + cost   # Substitution
            )

    return dist[-1][-1]


def filtrar(word: str) -> list:
    blocklist = ["problema", "falha", "erro", "conflito", "atraso", "parada", "complicacao", "aborrecimento",
                 "travado", "lotado", "calor", "abarrotado", "quente", "cheio", "morte", "quebrado", "demora", "transtorno"]

    bad_word = None
    for w in blocklist:
        dist = levenshtein_distance(word, w)

        if dist <= 1:
            bad_word = w

    return bad_word != None, word, bad_word


def salvar(dados: pd.DataFrame):
    dados.to_csv("filtro.csv", index=False)

def analise_lexica(words: list) -> list:
    # analise lexica para identificar palavras por categorias
    return words


if __name__ == "__main__":
    tweets = import_tweets()

    filtro = []
    for text in tweets:
        for word in text:
            p = filtrar(word)
            if p[0] == True:
                print(p)
                filtro.append(p[1])

    df = pd.DataFrame(filtro, columns=["Bad_Word"])
    salvar(df)
    # put_file_to_s3("filtro", df)
