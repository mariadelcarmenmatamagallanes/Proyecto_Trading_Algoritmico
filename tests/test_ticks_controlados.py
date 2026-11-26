"""Pruebas del generador de ticks controlados y del validador de velas (sin Kafka)."""

import sys
import unittest
from datetime import datetime, timezone
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "src"))
from generar_ticks_controlados import construir_ticks  # noqa: E402
from validar_velas import comparar  # noqa: E402

BASE = datetime(2026, 11, 27, 15, 0, tzinfo=timezone.utc)


def vela_desde_ticks(ticks, inicio):
    """Reproduce en Python lo que hace Spark: filtra, usa MID y ordena por hora del evento."""
    minuto = inicio[:16]
    validos = [t for t in ticks
               if t["symbol"] == "EURUSD" and t["bid"] > 0 and t["ask"] >= t["bid"]
               and t["timestamp"][:16] == minuto]
    validos.sort(key=lambda t: t["timestamp"])
    mids = [round((t["bid"] + t["ask"]) / 2, 5) for t in validos]
    return {"inicio": inicio, "open": mids[0], "high": max(mids), "low": min(mids),
            "close": mids[-1], "num_ticks": len(mids)}


class PruebasTicksControlados(unittest.TestCase):

    def setUp(self):
        self.ticks, self.esperadas = construir_ticks(BASE, 3)

    def test_tres_velas_esperadas(self):
        self.assertEqual([e["inicio"] for e in self.esperadas],
                         ["2026-11-27T15:00:00Z", "2026-11-27T15:01:00Z", "2026-11-27T15:02:00Z"])

    def test_esperadas_coinciden_con_simulacion_de_spark(self):
        for e in self.esperadas:
            simulada = vela_desde_ticks(self.ticks, e["inicio"])
            for campo in ("open", "high", "low", "close", "num_ticks"):
                self.assertAlmostEqual(simulada[campo], e[campo], places=9, msg=(e["inicio"], campo))

    def test_minuto_2_llega_al_reves(self):
        minuto2 = [t for t in self.ticks if t["timestamp"].startswith("2026-11-27T15:02")]
        self.assertGreater(minuto2[0]["timestamp"], minuto2[-1]["timestamp"])

    def test_incluye_ticks_que_deben_descartarse(self):
        self.assertTrue(any(t["symbol"] == "GBPUSD" for t in self.ticks))
        self.assertTrue(any(t["ask"] < t["bid"] for t in self.ticks))

    def test_tick_de_cierre_supera_watermark(self):
        self.assertEqual(self.ticks[-1]["timestamp"], "2026-11-27T15:03:15Z")

    def test_validador_aprueba_velas_correctas(self):
        aprobado, _ = comparar(self.esperadas, [dict(e) for e in self.esperadas])
        self.assertTrue(aprobado)

    def test_validador_detecta_error_y_faltante_y_repetida(self):
        malas = [dict(self.esperadas[0], close=1.0), dict(self.esperadas[1]), dict(self.esperadas[1])]
        aprobado, filas = comparar(self.esperadas, malas)
        self.assertFalse(aprobado)
        self.assertEqual([f[1] for f in filas], ["FALLA", "FALLA", "FALTA"])


if __name__ == "__main__":
    unittest.main()
