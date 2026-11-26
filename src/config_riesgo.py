"""Configuracion de riesgo para pruebas Paper Trading."""

SIMBOLO_PERMITIDO = "EURUSD"
CAPITAL_POR_OPERACION_USD = 1000.0
RIESGO_MAXIMO_TEORICO_USD = 20.0
STOP_LOSS = 0.02
TAKE_PROFIT = 0.04
MAX_POSICIONES = 1
VIGENCIA_SENAL_SEGUNDOS = 120
TOLERANCIA_FUTURA_SEGUNDOS = 30

# Paso 19.7: mientras sea False el ejecutor trabaja en Dry Run (no envía órdenes)
# y la diferencia entre posición lógica y posición IBKR solo se informa.
ORDENES_HABILITADAS = False
