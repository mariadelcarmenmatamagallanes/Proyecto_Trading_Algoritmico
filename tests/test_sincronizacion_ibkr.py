"""
Pruebas del paso 18.9 / 18.10 SIN TWS: se usa un IBKR falso que devuelve
las posiciones y órdenes que cada prueba necesita.

Ejecutar desde la raíz del proyecto:
  python -m unittest discover tests -v
"""

import sys
import unittest
from pathlib import Path
from types import SimpleNamespace as Obj

sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "src"))
from sincronizacion_ibkr import (  # noqa: E402
    control_antes_de_operar,
    ordenes_abiertas_eurusd,
    posicion_real_eurusd,
    revisar_sincronizacion,
)

EURUSD = Obj(secType="CASH", symbol="EUR", currency="USD")
GBPUSD = Obj(secType="CASH", symbol="GBP", currency="USD")


class IBFalso:
    def __init__(self, posiciones=(), ordenes=()):
        self._posiciones = list(posiciones)
        self._ordenes = list(ordenes)

    def positions(self):
        return self._posiciones

    def openTrades(self):
        return self._ordenes


def posicion(cantidad, contrato=EURUSD, cuenta="DU123"):
    return Obj(account=cuenta, contract=contrato, position=cantidad, avgCost=1.05)


class PruebasSincronizacion(unittest.TestCase):

    def test_sin_posiciones_es_cero(self):
        self.assertEqual(posicion_real_eurusd(IBFalso()), 0)

    def test_suma_solo_eurusd(self):
        ib = IBFalso([posicion(900), posicion(500, GBPUSD)])
        self.assertEqual(posicion_real_eurusd(ib), 900)

    def test_filtra_por_cuenta(self):
        ib = IBFalso([posicion(900, cuenta="DU123"), posicion(300, cuenta="DU999")])
        self.assertEqual(posicion_real_eurusd(ib, cuenta="DU123"), 900)

    def test_ordenes_abiertas_solo_eurusd(self):
        ib = IBFalso(ordenes=[Obj(contract=EURUSD), Obj(contract=GBPUSD)])
        self.assertEqual(len(ordenes_abiertas_eurusd(ib)), 1)

    def test_sincronizado_no_bloquea(self):
        bloquear, mensaje = revisar_sincronizacion(True, 900, ordenes_habilitadas=True)
        self.assertFalse(bloquear)
        self.assertIn("[SINCRONIZADO]", mensaje)

    def test_dry_run_diferencia_solo_informa(self):
        bloquear, mensaje = revisar_sincronizacion(True, 0, ordenes_habilitadas=False)
        self.assertFalse(bloquear)
        self.assertIn("[INFO DRY-RUN]", mensaje)

    def test_ordenes_habilitadas_diferencia_bloquea(self):
        bloquear, mensaje = revisar_sincronizacion(False, 900, ordenes_habilitadas=True)
        self.assertTrue(bloquear)
        self.assertIn("[BLOQUEADO]", mensaje)

    def test_rechaza_si_hay_orden_pendiente(self):
        ib = IBFalso(ordenes=[Obj(contract=EURUSD)])
        self.assertIsNotNone(control_antes_de_operar(ib, "BUY", ordenes_habilitadas=False))

    def test_dry_run_sin_pendientes_permite(self):
        self.assertIsNone(control_antes_de_operar(IBFalso(), "BUY", ordenes_habilitadas=False))

    def test_buy_rechazado_si_ibkr_ya_tiene_posicion(self):
        ib = IBFalso([posicion(900)])
        self.assertIsNotNone(control_antes_de_operar(ib, "BUY", ordenes_habilitadas=True))

    def test_sell_rechazado_si_ibkr_no_tiene_posicion(self):
        self.assertIsNotNone(control_antes_de_operar(IBFalso(), "SELL", ordenes_habilitadas=True))

    def test_sell_permitido_si_hay_posicion(self):
        ib = IBFalso([posicion(900)])
        self.assertIsNone(control_antes_de_operar(ib, "SELL", ordenes_habilitadas=True))


if __name__ == "__main__":
    unittest.main()
