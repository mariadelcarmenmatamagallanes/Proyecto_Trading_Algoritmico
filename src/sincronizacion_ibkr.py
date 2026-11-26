"""
Paso 18.9 y parte del 18.10: comparar lo que el ejecutor "cree" (SQLite) con lo que
IBKR informa realmente (posiciones y órdenes abiertas).

Todo funciona con la API en SOLO LECTURA: solo se consulta, nunca se envían órdenes.

Las funciones reciben el objeto `ib` (ib_async.IB) ya conectado. Por eso se pueden
probar sin TWS usando objetos falsos (ver tests/test_sincronizacion_ibkr.py).

Por qué importa:
  - En Dry Run el ejecutor no envía órdenes, así que IBKR siempre tendrá posición 0
    aunque el ejecutor "crea" que compró. Ahí la diferencia es normal y solo se informa.
  - Cuando se envíen órdenes Paper (paso 19.7), la posición lógica y la real DEBEN
    coincidir. Si no coinciden, el ejecutor se bloquea hasta revisarlo a mano.
"""

BASE, COTIZADA = "EUR", "USD"   # EURUSD en IBKR es un contrato CASH: symbol=EUR, currency=USD


def es_eurusd(contrato):
    return (
        getattr(contrato, "secType", None) == "CASH"
        and getattr(contrato, "symbol", None) == BASE
        and getattr(contrato, "currency", None) == COTIZADA
    )


def posicion_real_eurusd(ib, cuenta=None):
    """Cantidad de EUR en posición según IBKR (0 si no hay). Positiva = compra."""
    total = 0.0
    for posicion in ib.positions():
        if cuenta and posicion.account != cuenta:
            continue
        if es_eurusd(posicion.contract):
            total += float(posicion.position)
    return total


def ordenes_abiertas_eurusd(ib):
    """Órdenes de EURUSD que IBKR todavía no termina (enviadas, pendientes, parciales)."""
    return [t for t in ib.openTrades() if es_eurusd(t.contract)]


def revisar_sincronizacion(en_posicion_logica, posicion_real, ordenes_habilitadas):
    """
    Compara el estado lógico con el real. Devuelve (bloquear, mensaje).

    - Dry Run (ordenes_habilitadas=False): nunca bloquea; solo informa.
    - Órdenes Paper (True): bloquea si la posición lógica y la real no coinciden.
    """
    hay_posicion_real = abs(posicion_real) > 0
    resumen = (f"posición lógica (SQLite) = {'ABIERTA' if en_posicion_logica else 'CERRADA'}, "
               f"posición IBKR = {posicion_real:g} EUR")

    if en_posicion_logica == hay_posicion_real:
        return False, f"[SINCRONIZADO] {resumen}"

    if not ordenes_habilitadas:
        return False, (f"[INFO DRY-RUN] {resumen}. La diferencia es esperada porque "
                       f"en Dry Run no se envían órdenes a IBKR.")

    return True, (f"[BLOQUEADO] {resumen}. No coinciden: revisar la cuenta Paper en TWS "
                  f"y corregir el estado antes de operar.")


def control_antes_de_operar(ib, tipo, ordenes_habilitadas, cuenta=None):
    """
    Controles del 18.10 que dependen de IBKR. Devuelve un texto de rechazo o None.

    1. No operar si ya hay órdenes de EURUSD pendientes en IBKR (evita órdenes duplicadas).
    2. Con órdenes habilitadas:
       - BUY solo si IBKR no tiene ya una posición (máximo 1 posición, contando la real).
       - SELL solo si IBKR sí tiene una posición que cerrar.
    """
    pendientes = ordenes_abiertas_eurusd(ib)
    if pendientes:
        return f"Hay {len(pendientes)} orden(es) EURUSD pendiente(s) en IBKR"

    if not ordenes_habilitadas:
        return None

    real = posicion_real_eurusd(ib, cuenta)
    if tipo == "BUY" and real > 0:
        return "IBKR ya tiene una posición EURUSD abierta"
    if tipo == "SELL" and real <= 0:
        return "IBKR no tiene posición EURUSD para cerrar"
    return None
