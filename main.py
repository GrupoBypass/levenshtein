import pandas as pd
import boto3
import re
from datetime import datetime
from io import StringIO
from TokenType import TokenType

# ---------- Analisador Lexico ---------- #

padroes_problemas = [
    (r'\batraso\w*|\bdemor\w+', TokenType.ATRASO),
    (r'\blotad\w+|\bsuperlot\w+', TokenType.LOTACAO),
    (r'\bchei\w+', TokenType.LOTACAO),
    (r'\bfil\w+', TokenType.LOTACAO),
    (r'\bpane\w*|\bdefeit\w+|\bquebr\w+', TokenType.FALHA),
    (r'\bparalis\w+|\binterromp\w+', TokenType.INTERRUPCAO),
    (r'\bprotestp\w+|\bmanifest\w+', TokenType.INTERRUPCAO),
    (r'\btrava\w+|\bbloqu\w+', TokenType.INTERRUPCAO),
    (r'\bhorr[íi]vel|\bp[ée]ssim\w+', TokenType.RECLAMACAO),
    (r'\bhumilh\w+', TokenType.RECLAMACAO),
    (r'\blament\w+|\blament[AaÁá]\w+|\bcr[IiÍí]tic\w+', TokenType.RECLAMACAO),
    (r'\bcaos\w+|\bca[OoÓó]tic\w+', TokenType.RECLAMACAO),
    (r'\brui\w+', TokenType.RECLAMACAO),
    (r'\btranstorn\w+', TokenType.RECLAMACAO),
    (r'\bbom\b|\bótimo|\befici\w+', TokenType.ELOGIO)
]

padroes_gerais = [
    (r'[Ll]inha\s\d+', TokenType.LINHA),
    (r'[Ee]sta[çc][aã]o?\s\w+', TokenType.ESTACAO),
    (r'\btrem\w*|\bcomposi[çc][aã]o', TokenType.VEICULO),
    (r'\b[OoÔô]nibu\w*', TokenType.ONIBUS),
    (r'\d{1,2}[:h]\d{0,2}', TokenType.HORARIO),
    (r'\bhoje\b|\bontem\b|\d{1,2}/\d{1,2}', TokenType.DATA),
    (r'#[Cc][Pp][Tt][Mm]\w*', TokenType.HASHTAG),
    (r'@[Cc][Pp][Tt][Mm]\w*', TokenType.MENCIAO),
    (r'[.,!?;:]', TokenType.PONTUACAO)
]

re_problemas = [(re.compile(padrao, re.IGNORECASE), tipo)
                        for padrao, tipo in padroes_problemas]
re_gerais = [(re.compile(padrao, re.IGNORECASE), tipo)
                    for padrao, tipo in padroes_gerais]

def tokenize( tweet):
    tokens = []
    pos = 0
    tweet = tweet.strip()

    while pos < len(tweet):
        if tweet[pos].isspace():
            pos += 1
            continue

        matched = False

        for regex, tipo in re_problemas:
            match = regex.match(tweet, pos)
            if match:
                tokens.append((tipo, match.group()))
                pos = match.end()
                matched = True
                break

        if matched:
            continue

        for regex, tipo in re_gerais:
            match = regex.match(tweet, pos)
            if match:
                tokens.append((tipo, match.group()))
                pos = match.end()
                matched = True
                break

        if not matched:
            next_space = pos
            while next_space < len(tweet) and not tweet[next_space].isspace():
                next_space += 1

            palavra = tweet[pos:next_space]
            tokens.append((TokenType.PALAVRA, palavra))
            pos = next_space

    return tokens

def analisar_problemas(tweet):
    tokens = tokenize(tweet)
    problemas = []

    for tipo, valor in tokens:
        if tipo in [TokenType.ATRASO, TokenType.LOTACAO,
                    TokenType.FALHA, TokenType.INTERRUPCAO,
                    TokenType.RECLAMACAO]:
            problemas.append((tipo, valor))

    return problemas

def import_tweets() -> list:
    df = pd.read_csv('tweets.csv')
    tweets = df['text'].tolist()

    return tweets


# ---------- Levenshtein ---------- #

def put_file_to_s3(file_name, df: pd.DataFrame):
    s3 = boto3.client('s3',
                      aws_access_key_id='',
                      aws_secret_access_key='',
                      aws_session_token='')

    bucket_name = 'client-bypass'
    file_key = f'data/{file_name}.csv'

    csv_buffer = StringIO()
    df.to_csv(csv_buffer, index=False)

    s3.put_object(Bucket=bucket_name, Key=file_key, Body=csv_buffer.getvalue())


def import_tweets_ofensivos() -> list:
    df = pd.read_csv('tweets_tratados.csv')

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


def salvar(dados: pd.DataFrame, nome: str):
    dados.to_csv(nome, index=False)


if __name__ == "__main__":
# Etapa 1
    tweets = import_tweets()

    tweets_ofensivos = []
    for i, tweet in enumerate(tweets, 1):
        problemas = analisar_problemas(tweet)

        if problemas:
            tweets_ofensivos.append(tweet)

    salvar(pd.DataFrame(tweets_ofensivos, columns=["text"]), "tweets_tratados.csv")

# Etapa 2
    tweets = import_tweets_ofensivos()

    filtro = []
    for text in tweets:
        for word in text:
            p = filtrar(word)
            if p[0] == True:
                filtro.append(p[1])

    df = pd.DataFrame(filtro, columns=["Bad_Word"])
    salvar(df, "wordcloud.csv")

    put_file_to_s3("wordcloud-" + str(datetime.now().date()), df)
