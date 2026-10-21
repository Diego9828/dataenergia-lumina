"""Convierte el calendario oficial de la tarifa dinámica (dToU 2013) a CSV para BigQuery.

Fuente: London Datastore, "SmartMeter Energy Consumption Data in London Households",
archivo Tariffs.xlsx (UK Power Networks · Low Carbon London, licencia CC BY 4.0).
Trae un nivel (High / Normal / Low) por cada media hora de 2013: 17,520 filas. Todos los días
tienen 48 medias horas, incluso los de cambio de hora, así que se interpreta en GMT/UTC
(igual que las lecturas de los medidores).

Precios publicados del ensayo (p/kWh): High 67.20 · Normal 11.76 · Low 3.99; tarifa plana 14.228.
Uso:  python src/ingestion/tarifas_dtou.py Tariffs.xlsx tarifas_dtou.csv
"""
import sys

import pandas as pd

PRECIOS_P_KWH = {"High": 67.20, "Normal": 11.76, "Low": 3.99}
TARIFA_PLANA_P_KWH = 14.228


def main(entrada: str, salida: str) -> None:
    df = pd.read_excel(entrada, sheet_name=0)
    df.columns = ["ts_utc", "nivel"]
    df["nivel"] = df["nivel"].str.strip()
    desconocidos = set(df["nivel"]) - set(PRECIOS_P_KWH)
    if desconocidos:
        raise SystemExit(f"Niveles no esperados: {desconocidos}")
    if len(df) != 17_520 or df["ts_utc"].duplicated().any():
        raise SystemExit(f"Se esperaban 17,520 medias horas únicas de 2013 y hay {len(df)}")
    df["precio_p_kwh"] = df["nivel"].map(PRECIOS_P_KWH)
    df["precio_plano_p_kwh"] = TARIFA_PLANA_P_KWH
    df["fecha"] = df["ts_utc"].dt.date
    df["ts_utc"] = df["ts_utc"].dt.strftime("%Y-%m-%d %H:%M:%S")
    df[["ts_utc", "fecha", "nivel", "precio_p_kwh", "precio_plano_p_kwh"]].to_csv(salida, index=False)
    print(f"[tarifas] {len(df):,} medias horas -> {salida}: {df['nivel'].value_counts().to_dict()}")


if __name__ == "__main__":
    main(sys.argv[1], sys.argv[2])
