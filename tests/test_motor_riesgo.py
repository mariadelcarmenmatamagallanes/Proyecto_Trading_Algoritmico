"""Pruebas aisladas del motor actual; sin conexiones ni archivos de datos reales."""
import contextlib
import io
import runpy
import sqlite3
import sys
import unittest
from datetime import datetime, timedelta, timezone
from pathlib import Path
from types import SimpleNamespace, ModuleType
from unittest.mock import Mock, patch

SRC = Path(__file__).resolve().parent.parent / 'src'
if not SRC.is_dir():
    SRC = Path(__file__).resolve().parent
sys.path.insert(0, str(SRC))
import calculo_riesgo as riesgo
import seguridad_ibkr as seguridad
import sincronizacion_ibkr as sync

class Memoria(sqlite3.Connection):
    def close(self):
        pass  # el script cierra al salir; conservamos la BD para probar procesar

class Motor(unittest.TestCase):
    def setUp(self):
        self.db = sqlite3.connect(':memory:', factory=Memoria)
        self.addCleanup(sqlite3.Connection.close, self.db)
        ib = Mock()
        ib.positions.return_value = []
        ib.openTrades.return_value = []
        consumer = Mock()
        consumer.poll.side_effect = KeyboardInterrupt
        kafka = ModuleType('confluent_kafka')
        kafka.Consumer = Mock(return_value=consumer)
        broker = ModuleType('ib_async')
        broker.IB = Mock(return_value=ib)
        with patch.dict(sys.modules, {'confluent_kafka': kafka, 'ib_async': broker}), \
             patch.object(seguridad, 'verificar_conexion_paper', return_value=True), \
             patch('sqlite3.connect', return_value=self.db), \
             patch.object(Path, 'mkdir'), contextlib.redirect_stdout(io.StringIO()):
            mod = runpy.run_path(str(SRC / 'ejecutor_riesgo.py'))
        self.procesar = mod['procesar']
        self.validar = mod['validar']
        self.ib = ib
        self.s = dict(id_algoritmo='SMA_10_50', symbol='EURUSD', signal='BUY', precio_referencia=1.08,
                      timestamp=datetime.now(timezone.utc).isoformat())

    def test_invalidas(self):
        for cambios in ({'symbol':'GBPUSD'}, {'signal':'HOLD'}, {'id_algoritmo':''}, {'precio_referencia':0},
                        {'precio_referencia':float('nan')}, {'precio_referencia':float('inf')}, {'timestamp':'mal'},
                        {'timestamp':'2026-01-01T00:00:00'}):
            with self.subTest(cambios=cambios):
                self.assertTrue(self.procesar(dict(self.s, **cambios)).startswith('[RECHAZADA]'))
        self.assertIsNotNone(self.validar([]))
        self.assertIsNotNone(self.validar({}))
        self.assertEqual(self.db.execute('SELECT count(*) FROM aceptadas').fetchone()[0], 0)

    def test_vigencia(self):
        for segundos in (-121, 31):
            s = dict(self.s, timestamp=(datetime.now(timezone.utc)+timedelta(seconds=segundos)).isoformat())
            self.assertIn('antigua o futura', self.procesar(s))

    def test_ciclo_y_duplicidad(self):
        self.assertIn('[DRY-RUN]', self.procesar(self.s))
        self.assertIn('duplicada', self.procesar(self.s))
        otra = dict(self.s, timestamp=datetime.now(timezone.utc).isoformat(timespec='seconds'))
        self.assertIn('Ya existe', self.procesar(otra))
        venta = dict(self.s, signal='SELL')
        self.assertIn('[DRY-RUN]', self.procesar(venta))
        self.assertIn('No existe', self.procesar(dict(otra, signal='SELL')))
        self.assertEqual(self.db.execute('SELECT en_posicion FROM estado').fetchone()[0], 0)
        self.assertEqual(self.db.execute('SELECT count(*) FROM aceptadas').fetchone()[0], 2)
        self.ib.placeOrder.assert_not_called()

    def test_error_ibkr(self):
        self.ib.openTrades.side_effect = RuntimeError('simulado')
        self.assertIn('No se pudo consultar IBKR', self.procesar(self.s))
        self.assertEqual(self.db.execute('SELECT count(*) FROM aceptadas').fetchone()[0], 0)

    def test_duplicado_timestamp_equivalente(self):
        self.assertIn('[DRY-RUN]', self.procesar(self.s))
        self.assertIn('[DRY-RUN]', self.procesar(dict(self.s, signal='SELL')))
        equivalente = dict(self.s, timestamp=self.s['timestamp'].replace('+00:00','Z'))
        self.assertIn('duplicada', self.procesar(equivalente))

class Controles(unittest.TestCase):
    def test_riesgo(self):
        for precio in (0.01, 1.08, 999, 1000):
            r = riesgo.calcular_riesgo(precio)
            self.assertLessEqual(r['exposicion_usd'], 1000)
            self.assertLessEqual(r['riesgo_teorico_usd'], 20)
            self.assertTrue(r['riesgo_autorizado'])
        for precio in (0, -1, float('nan'), float('inf'), 1001):
            with self.assertRaises(ValueError):
                riesgo.calcular_riesgo(precio)

    def test_cuenta(self):
        ib = Mock()
        ib.isConnected.return_value = True
        ib.managedAccounts.return_value = ['DU123']
        for contenido, esperado in [('IBKR_PAPER_ACCOUNT=DU123', True), ('IBKR_PAPER_ACCOUNT=DU456', False), ('', False), ('IBKR_PAPER_ACCOUNT=DU123\nIBKR_PAPER_ACCOUNT=DU123', False)]:
            with patch.object(Path, 'read_text', return_value=contenido):
                self.assertEqual(seguridad.verificar_conexion_paper(ib), esperado)
        ib.isConnected.return_value = False
        self.assertFalse(seguridad.verificar_conexion_paper(ib))
        self.assertFalse(seguridad.validar_cuentas_paper(['U123']))

    def test_posicion_negativa(self):
        ib = Mock()
        ib.openTrades.return_value = []
        ib.positions.return_value = [SimpleNamespace(account='DU123', position=-10, contract=SimpleNamespace(secType='CASH',symbol='EUR',currency='USD'))]
        self.assertIsNotNone(sync.control_antes_de_operar(ib,'BUY',True))

    def test_sincronizacion_negativa(self):
        self.assertTrue(sync.revisar_sincronizacion(True,-10,True)[0])

if __name__ == '__main__':
    unittest.main(verbosity=2)
