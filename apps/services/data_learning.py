import pandas as pd
from pathlib import Path
import logging

from apps.common.parquet_exporter import export_to_parquet
from apps.common.json_exporter import export_to_json

logger = logging.getLogger(__name__)

def process_learning_data(base_input_path: Path | str, base_output_path: Path | str) -> bool:
    input_path = Path(str(base_input_path) + ".parquet")

    if not input_path.exists():
        logger.error(f"File input tidak ditemukan: {input_path}")
        return False

    try:
        df = pd.read_parquet(input_path)
        
        df['time'] = pd.to_datetime(df['time'], format='%d-%m-%Y %H:%M:%S', errors='coerce')
        df = df.dropna(subset=['time'])

        df_pivot = df.pivot_table(
            index='time',
            columns='vparam',
            values=['nvalue', 'catg', 'threshold'],
            aggfunc='first'
        )

        new_columns = []
        for val, param in df_pivot.columns:
            prefix = 'critical' if val == 'catg' else 'treshold' if val == 'threshold' else val
            new_columns.append(f"{prefix}_{param}")
        df_pivot.columns = new_columns

        df_pivot = df_pivot.sort_index().ffill().bfill().reset_index()

        df_pivot['time'] = df_pivot['time'].dt.strftime('%d-%m-%Y %H:%M:%S')

        expected_columns = [
            'time', 
            'nvalue_CUSHION-POSITION', 'nvalue_INJ-MAX-PRESSURE', 'nvalue_NH1-TEMP', 'nvalue_NH2-TEMP', 
            'critical_CUSHION-POSITION', 'critical_INJ-MAX-PRESSURE', 'critical_NH1-TEMP', 'critical_NH2-TEMP', 
            'treshold_CUSHION-POSITION', 'treshold_INJ-MAX-PRESSURE', 'treshold_NH1-TEMP', 'treshold_NH2-TEMP'
        ]
        final_cols = [col for col in expected_columns if col in df_pivot.columns]
        df_final = df_pivot[final_cols]

    except Exception as e:
        logger.error(f"Kesalahan data learning: {e}")
        return False

    sukses_pq = export_to_parquet(df_final, base_output_path)
    sukses_js = export_to_json(df_final, base_output_path)
    
    return sukses_pq and sukses_js