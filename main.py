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


def get_string() -> list:
    df = pd.read_csv('tweets.csv')

    return df['text'].str.split()


def levenshtein_distance(s1, s2):
    """
    Calculate the Levenshtein distance between two strings.

    Parameters:
    s1 (str): First string
    s2 (str): Second string

    Returns:
    int: The Levenshtein distance between s1 and s2
    """

    # Create a distance matrix with dimensions (len(s1)+1) x (len(s2)+1)
    rows = len(s1) + 1
    cols = len(s2) + 1
    dist = [[0 for _ in range(cols)] for _ in range(rows)]

    # Initialize the first row and column with incremental values
    # This represents the cost of inserting all characters of the other string
    for i in range(1, rows):
        dist[i][0] = i
    for j in range(1, cols):
        dist[0][j] = j

    # Fill in the distance matrix
    for i in range(1, rows):
        for j in range(1, cols):
            # If characters are the same, no cost for substitution
            cost = 0 if s1[i-1] == s2[j-1] else 1

            # Calculate minimum cost from three possible operations:
            # 1. Deletion (left cell)
            # 2. Insertion (top cell)
            # 3. Substitution (diagonal cell)
            dist[i][j] = min(
                dist[i-1][j] + 1,      # Deletion
                dist[i][j-1] + 1,      # Insertion
                dist[i-1][j-1] + cost   # Substitution
            )

    # The bottom-right cell contains the Levenshtein distance
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


if __name__ == "__main__":
    tweets = get_string()

    filtro = []
    for text in tweets:
        for word in text:
            p = filtrar(word)
            if p[0] == True:
                print(p)
                filtro.append(p[1])

    df = pd.DataFrame(filtro, columns=["Bad_Word"])
    salvar(df)
    put_file_to_s3("filtro", df)
