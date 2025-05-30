import pandas as pd
import re
from enum import Enum, auto


class TokenType(Enum):
    # Entidades
    LINHA = auto()          # Ex: Linha 7, Linha 10
    ESTACAO = auto()        # Ex: Estação Brás, Luz
    VEICULO = auto()        # Ex: trem, composição

    # Problemas
    ATRASO = auto()         # Ex: atraso, demora
    LOTACAO = auto()        # Ex: lotado, superlotação
    FALHA = auto()          # Ex: pane, defeito
    INTERRUPCAO = auto()    # Ex: paralisação, interrompido

    # Temporal
    HORARIO = auto()        # Ex: 8:30, 17h
    DATA = auto()           # Ex: 12/05, hoje

    # Sentimento
    RECLAMACAO = auto()     # Ex: horrível, péssimo
    ELOGIO = auto()         # Ex: bom, eficiente

    # Outros
    HASHTAG = auto()        # Ex: #CPTM
    MENCIAO = auto()        # Ex: @cptmoficial
    PALAVRA = auto()        # Palavras não classificadas
    PONTUACAO = auto()      # Pontuação
    ONIBUS = auto()         # Ex: ônibus, coletivo


class TweetLexer:
    def __init__(self):
        # Expressões regulares para problemas comuns
        self.padroes_problemas = [
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

        # Padrões gerais
        self.padroes_gerais = [
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

        # Compilar todas as regex
        self.re_problemas = [(re.compile(padrao, re.IGNORECASE), tipo)
                             for padrao, tipo in self.padroes_problemas]
        self.re_gerais = [(re.compile(padrao, re.IGNORECASE), tipo)
                          for padrao, tipo in self.padroes_gerais]

    def tokenize(self, tweet):
        tokens = []
        pos = 0
        tweet = tweet.strip()

        while pos < len(tweet):
            # Pular espaços em branco
            if tweet[pos].isspace():
                pos += 1
                continue

            matched = False

            # Verificar padrões de problemas primeiro
            for regex, tipo in self.re_problemas:
                match = regex.match(tweet, pos)
                if match:
                    tokens.append((tipo, match.group()))
                    pos = match.end()
                    matched = True
                    break

            if matched:
                continue

            # Verificar padrões gerais
            for regex, tipo in self.re_gerais:
                match = regex.match(tweet, pos)
                if match:
                    tokens.append((tipo, match.group()))
                    pos = match.end()
                    matched = True
                    break

            if not matched:
                # Capturar palavras não classificadas
                next_space = pos
                while next_space < len(tweet) and not tweet[next_space].isspace():
                    next_space += 1

                palavra = tweet[pos:next_space]
                tokens.append((TokenType.PALAVRA, palavra))
                pos = next_space

        return tokens

    def analisar_problemas(self, tweet):
        tokens = self.tokenize(tweet)
        problemas = []

        for tipo, valor in tokens:
            if tipo in [TokenType.ATRASO, TokenType.LOTACAO,
                        TokenType.FALHA, TokenType.INTERRUPCAO,
                        TokenType.RECLAMACAO]:
                problemas.append((tipo, valor))

        return problemas


# Exemplo de uso
if __name__ == "__main__":
    lexer = TweetLexer()

    df = pd.read_csv('tweets.csv')
    tweets = df['text'].tolist()

    print("ANÁLISE DE TWEETS SOBRE CPTM")
    print("============================")

    for i, tweet in enumerate(tweets, 1):
        print(f"\nTweet {i}: '{tweet}'")
        problemas = lexer.analisar_problemas(tweet)

        if problemas:
            print("Problemas encontrados:")
            for tipo, valor in problemas:
                print(f"- {tipo.name}: {valor}")
        else:
            print("Nenhum problema identificado.")
