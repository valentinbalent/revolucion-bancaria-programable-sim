# Revolución Bancaria Programable — Simulation (ABM/DE)

Repo reproducible para simulación A/B (Mundo A vs Mundo B) con:
- DE (SimPy) + policies tipo ABM
- CRN (Common Random Numbers) para A/B pareado por seed
- Artifacts por corrida: tx_log.csv, event_log.csv, kpi_run.json, config_used.json

## Quickstart (macOS)

```bash
python3 -m venv .venv
source .venv/bin/activate
python -m pip install --upgrade pip
pip install -e ".[dev]"
```

## License
MIT — ver LICENSE.
