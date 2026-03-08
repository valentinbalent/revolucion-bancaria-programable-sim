# Baseline Calibration Pack

## Objetivo
Dejar explícito y audit-able el baseline (Mundo A / Mundo B) mediante una tabla única:
`calibration/parameters.csv`

Esto permite:
- reproducibilidad
- trazabilidad de supuestos
- sensibilidad (qué se mueve y qué no)
- defensa metodológica (DSR + simulation reporting)

## Tiers (T1/T2/T3)
### T1 — Externamente anclado
Parámetros fijados por fuente externa fuerte (paper, BIS/CPMI/IMF, estándar, dataset).
Cambios raros y siempre justificables.

### T2 — Estimado / acotado
Parámetros calibrados o inferidos con rango plausible.
Se permite mover dentro de rango; sensibilidad típicamente Y.

### T3 — Protocolo / decisión de diseño
Decisiones del protocolo experimental (seeds, márgenes NI, pesos IFS, run_id_salt).
Se “congelan” para la tesis.

## Mapeo a configs
`param_name` usa dotted paths que corresponden a keys del JSON.
Ejemplo: `repro.run_id_salt` -> `cfg["repro"]["run_id_salt"]`.

## Sensitivity (Y/N)
Y: entra en robustez/sensibilidad
N: fijo en runs de tesis (freeze)
