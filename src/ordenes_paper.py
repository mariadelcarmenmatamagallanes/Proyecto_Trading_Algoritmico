"""Ejecuta órdenes EURUSD en la cuenta Paper autorizada y registra su estado."""

import json
import math
import os
import time
from datetime import datetime
from decimal import Decimal, ROUND_CEILING, ROUND_FLOOR
from hashlib import sha256

from ib_async import ExecutionFilter, Forex, LimitOrder

from config_riesgo import SIMBOLO_PERMITIDO
from seguridad_ibkr import verificar_conexion_paper
from sincronizacion_ibkr import es_eurusd


ESTADOS_CANCELADOS = {"Cancelled", "ApiCancelled"}


def saldo_eur(ib, cuenta):
    """Obtiene una instantánea del efectivo por divisa de la cuenta Paper."""
    solicitud = ib.client.getReqId()
    recibir = ib.wrapper.accountUpdateMulti
    terminar = ib.wrapper.accountUpdateMultiEnd
    saldos = {}
    completo = False

    def cuenta_recibida(req, cuenta_ib, modelo, etiqueta, valor, moneda):
        recibir(req, cuenta_ib, modelo, etiqueta, valor, moneda)
        if req == solicitud and cuenta_ib == cuenta and not modelo and etiqueta == "$LEDGER-TotalCashBalance":
            saldos[moneda] = Decimal(valor)

    def descarga_completa(req):
        nonlocal completo
        terminar(req)
        if req == solicitud:
            completo = True

    ib.wrapper.accountUpdateMulti = cuenta_recibida
    ib.wrapper.accountUpdateMultiEnd = descarga_completa
    try:
        ib.client.reqAccountUpdatesMulti(solicitud, cuenta, "", True)
        limite = time.monotonic() + 6
        while ib.isConnected() and not completo and time.monotonic() < limite:
            ib.sleep(.2)
        if not completo or "USD" not in saldos or not all(v.is_finite() for v in saldos.values()):
            raise RuntimeError("IBKR no confirmó un saldo de divisas completo para la cuenta Paper")
        # TWS puede omitir la fila EUR cuando su saldo es cero.
        return saldos.get("EUR", Decimal(0))
    finally:
        ib.wrapper.accountUpdateMulti = recibir
        ib.wrapper.accountUpdateMultiEnd = terminar
        if ib.isConnected():
            ib.client.cancelAccountUpdatesMulti(solicitud)


class OrdenesPaper:
    def __init__(self, ib, db, posicion_abierta):
        self.ib = ib
        self.db = db
        self.cuenta = None
        self.cuenta = self.verificar_cuenta()
        with db:
            db.execute("""
                CREATE TABLE IF NOT EXISTS paper_posicion (
                    id INTEGER PRIMARY KEY CHECK (id = 1),
                    cuenta TEXT NOT NULL,
                    cantidad INTEGER NOT NULL CHECK (cantidad >= 0)
                )
            """)
            db.execute("""
                CREATE TABLE IF NOT EXISTS paper_ordenes (
                    clave TEXT PRIMARY KEY,
                    referencia TEXT NOT NULL UNIQUE,
                    tipo TEXT NOT NULL,
                    cantidad INTEGER NOT NULL CHECK (cantidad > 0),
                    limite REAL NOT NULL,
                    estado TEXT NOT NULL CHECK (estado IN ('pendiente', 'cancelada', 'ejecutada'))
                )
            """)
            db.execute("INSERT OR IGNORE INTO paper_posicion VALUES (1, ?, 0)", (self.cuenta,))

        cuenta, cantidad = db.execute("SELECT cuenta, cantidad FROM paper_posicion").fetchone()
        if cuenta != self.cuenta or bool(cantidad) != posicion_abierta:
            raise RuntimeError("La cuenta o la posición local Paper no coincide con SQLite")
        datos = ib.reqContractDetails(Forex(SIMBOLO_PERMITIDO))
        if len(datos) != 1 or not es_eurusd(datos[0].contract) or datos[0].contract.conId <= 0:
            raise RuntimeError("IBKR no identificó un único contrato EURUSD")
        self.contrato = datos[0].contract
        self.incremento = Decimal(str(datos[0].minTick))
        if not self.incremento.is_finite() or self.incremento <= 0:
            raise RuntimeError("Incremento mínimo de EURUSD inválido")

        pendientes = db.execute(
            "SELECT clave, referencia, tipo, cantidad, limite FROM paper_ordenes WHERE estado = 'pendiente'"
        ).fetchall()
        referencia = os.environ.get("RECUPERAR_BUY_PAPER")
        if pendientes:
            if (len(pendientes) != 1 or cantidad or pendientes[0][1] != referencia
                    or pendientes[0][2] != "BUY"):
                raise RuntimeError(f"Orden Paper pendiente de conciliación: {pendientes[0][1]}")
            cantidad = self.recuperar_compra(pendientes[0])
        elif referencia:
            raise RuntimeError("La referencia indicada ya no está pendiente de conciliación")
        self.comprobar_broker(cantidad)

    def verificar_cuenta(self):
        if not verificar_conexion_paper(self.ib):
            raise RuntimeError("Conexión con la cuenta Paper no autorizada")
        cuentas = self.ib.managedAccounts()
        if len(cuentas) != 1 or (self.cuenta is not None and cuentas[0] != self.cuenta):
            raise RuntimeError("La cuenta Paper cambió durante la sesión")
        return cuentas[0]

    def comprobar_broker(self, esperada, anterior=None, referencia=None):
        vencimiento = time.monotonic() + 15 if referencia is not None else None
        while True:
            self.verificar_cuenta()
            pendientes = [orden for orden in self.ib.reqAllOpenOrders() if es_eurusd(orden.contract)]
            if pendientes and (referencia is None or any(o.order.orderRef != referencia for o in pendientes)):
                raise RuntimeError("Hay una orden EURUSD pendiente en TWS")
            cantidad = saldo_eur(self.ib, self.cuenta)
            if cantidad == esperada and not pendientes:
                return
            # El fill, la lista de órdenes y el efectivo pueden actualizarse en momentos diferentes.
            if vencimiento is None or cantidad not in (anterior, esperada) or time.monotonic() >= vencimiento:
                if pendientes:
                    raise RuntimeError(f"TWS todavía devuelve abierta la orden ejecutada {referencia}")
                raise RuntimeError(f"Saldo Paper sin conciliar: SQLite/orden {esperada} EUR, IBKR {cantidad} EUR")
            self.ib.sleep(.75)

    def recuperar_compra(self, pendiente):
        clave, referencia, _, cantidad, limite = pendiente
        algoritmo, simbolo, tipo, fecha = json.loads(clave)
        if not algoritmo.startswith("PRUEBA_PAPER_") or (simbolo, tipo) != ("EURUSD", "BUY"):
            raise RuntimeError("La recuperación está reservada para el BUY de una prueba controlada")
        inicio = datetime.fromisoformat(fecha)
        ejecuciones = [
            e.execution for e in self.ib.reqExecutions(ExecutionFilter(acctCode=self.cuenta))
            if e.execution.acctNumber == self.cuenta and es_eurusd(e.contract) and e.execution.time >= inicio
        ]
        if not ejecuciones or any(
            e.orderRef != referencia or e.side != "BOT" or not (0 < e.shares <= cantidad)
            or not (0 < e.price <= limite) for e in ejecuciones
        ) or sum(e.shares for e in ejecuciones) != cantidad:
            raise RuntimeError(f"IBKR no confirmó exclusivamente el BUY completo {referencia}")
        self.comprobar_broker(cantidad)
        precio = sum(e.shares * e.price for e in ejecuciones) / cantidad

        with self.db:
            self.guardar_ejecucion(clave, cantidad)
            self.db.execute("INSERT INTO aceptadas VALUES (?, 'BUY', ?)", (clave, precio))
            self.db.execute("UPDATE estado SET en_posicion = 1 WHERE id = 1")
        print(f"[PAPER] BUY existente conciliado: {cantidad} EUR ({referencia})", flush=True)
        return cantidad

    def ya_registrada(self, clave):
        return self.db.execute(
            "SELECT 1 FROM paper_ordenes WHERE clave = ?", (clave,)
        ).fetchone() is not None

    def enviar(self, clave, tipo, precio, cantidad_compra, fecha_limite):
        cantidad_actual, = self.db.execute("SELECT cantidad FROM paper_posicion").fetchone()
        cantidad = cantidad_compra if tipo == "BUY" else cantidad_actual
        if (tipo == "BUY" and cantidad_actual != 0) or not cantidad or cantidad <= 0:
            raise RuntimeError("Cantidad incompatible con la posición Paper")

        redondeo = ROUND_FLOOR if tipo == "BUY" else ROUND_CEILING
        pasos = (Decimal(str(precio)) / self.incremento).to_integral_value(rounding=redondeo)
        limite = float(pasos * self.incremento)
        if not math.isfinite(limite) or limite <= 0:
            raise RuntimeError("Precio límite inválido para EURUSD")

        referencia = "TA-" + sha256(clave.encode("utf-8")).hexdigest()[:24]
        orden = LimitOrder(tipo, cantidad, limite, account=self.cuenta, tif="IOC", orderRef=referencia)
        self.comprobar_broker(cantidad_actual)

        # Persistir la intención antes de llamar al bróker. Si el envío es incierto,
        # el reinicio bloquea el reenvío hasta conciliar esta referencia en TWS.
        with self.db:
            self.db.execute("BEGIN IMMEDIATE")
            actual, = self.db.execute("SELECT cantidad FROM paper_posicion").fetchone()
            if actual != cantidad_actual or self.db.execute(
                "SELECT 1 FROM paper_ordenes WHERE estado = 'pendiente'"
            ).fetchone():
                raise RuntimeError("Otra orden Paper cambió el estado local")
            self.db.execute(
                "INSERT INTO paper_ordenes VALUES (?, ?, ?, ?, ?, 'pendiente')",
                (clave, referencia, tipo, cantidad, limite),
            )

        self.verificar_cuenta()
        if time.time() > fecha_limite:
            with self.db:
                self.db.execute("UPDATE paper_ordenes SET estado = 'cancelada' WHERE clave = ?", (clave,))
            return None, "La señal venció antes del envío"

        trade = self.ib.placeOrder(self.contrato, orden)
        vencimiento = time.monotonic() + 15
        estado = trade.orderStatus
        while estado.status not in ESTADOS_CANCELADOS | {"Filled", "Inactive"}:
            self.verificar_cuenta()
            restante = vencimiento - time.monotonic()
            if restante <= 0:
                raise RuntimeError(f"Sin confirmación final de la orden Paper {referencia}")
            self.ib.waitOnUpdate(timeout=min(restante, 1))
            estado = trade.orderStatus

        if estado.status in ESTADOS_CANCELADOS and estado.filled == 0:
            self.comprobar_broker(cantidad_actual)
            with self.db:
                self.db.execute("UPDATE paper_ordenes SET estado = 'cancelada' WHERE clave = ?", (clave,))
            return None, f"Orden Paper {referencia} cancelada sin ejecución"

        if (estado.status != "Filled" or estado.filled != cantidad or estado.remaining != 0
                or not math.isfinite(estado.avgFillPrice) or estado.avgFillPrice <= 0):
            raise RuntimeError(f"Orden Paper {referencia} requiere conciliación: {estado.status}, "
                               f"{estado.filled}/{cantidad} EUR ejecutados")

        nueva_cantidad = cantidad if tipo == "BUY" else 0
        self.comprobar_broker(nueva_cantidad, anterior=cantidad_actual, referencia=referencia)
        return (nueva_cantidad, estado.avgFillPrice), referencia

    def guardar_ejecucion(self, clave, cantidad):
        # El llamador confirma estos cambios y el registro de la señal juntos.
        actualizada = self.db.execute(
            "UPDATE paper_ordenes SET estado = 'ejecutada' WHERE clave = ? AND estado = 'pendiente'",
            (clave,),
        )
        if actualizada.rowcount != 1:
            raise RuntimeError("Falta el registro pendiente de la orden Paper ejecutada")
        self.db.execute("UPDATE paper_posicion SET cantidad = ? WHERE id = 1", (cantidad,))
