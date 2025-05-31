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
