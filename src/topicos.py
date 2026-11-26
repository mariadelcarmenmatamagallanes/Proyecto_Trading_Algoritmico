"""
Nombres de los tópicos Kafka, configurables por variable de entorno.

Sin variables definidas se usan los tópicos oficiales del profesor:
    ticks, velas, signals

Para pruebas aisladas con datos ficticios, definir antes de ejecutar, por ejemplo:
    PowerShell:  $env:TOPICO_TICKS="ticks_prueba"
    Git Bash:    export TOPICO_TICKS=ticks_prueba
"""

import os

TOPICO_TICKS = os.environ.get("TOPICO_TICKS", "ticks")
TOPICO_VELAS = os.environ.get("TOPICO_VELAS", "velas")
TOPICO_SIGNALS = os.environ.get("TOPICO_SIGNALS", "signals")

OFICIALES = ("ticks", "velas", "signals")


def modo_prueba():
    """True si algún tópico es distinto del oficial."""
    return (TOPICO_TICKS, TOPICO_VELAS, TOPICO_SIGNALS) != OFICIALES


def describir():
    modo = "PRUEBA (aislado)" if modo_prueba() else "OFICIAL"
    return f"Tópicos [{modo}]: ticks={TOPICO_TICKS} velas={TOPICO_VELAS} signals={TOPICO_SIGNALS}"
