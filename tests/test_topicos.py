"""Pruebas de la configuración de tópicos por variable de entorno."""

import os
import subprocess
import sys
import unittest
from pathlib import Path

SRC = Path(__file__).resolve().parent.parent / "src"
CODIGO = "import topicos; print(topicos.TOPICO_TICKS, topicos.TOPICO_VELAS, topicos.TOPICO_SIGNALS, topicos.modo_prueba())"


def leer(variables):
    entorno = {k: v for k, v in os.environ.items() if not k.startswith("TOPICO_")}
    entorno.update(variables)
    salida = subprocess.run([sys.executable, "-c", CODIGO], cwd=SRC, env=entorno,
                            capture_output=True, text=True, check=True)
    return salida.stdout.split()


class PruebasTopicos(unittest.TestCase):

    def test_sin_variables_usa_oficiales(self):
        self.assertEqual(leer({}), ["ticks", "velas", "signals", "False"])

    def test_con_variables_usa_prueba(self):
        resultado = leer({"TOPICO_TICKS": "ticks_prueba", "TOPICO_VELAS": "velas_prueba",
                          "TOPICO_SIGNALS": "signals_prueba"})
        self.assertEqual(resultado, ["ticks_prueba", "velas_prueba", "signals_prueba", "True"])


if __name__ == "__main__":
    unittest.main()
