import logging
from pathlib import Path

from apps.services.data_preparation import process_and_merge_json 
from apps.services.data_cleansing import apply_cleansing_rules
from apps.services.data_learning import process_learning_data

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)

def main():
    base_dir = Path(__file__).resolve().parent
    
    # SETUP FOLDER
    raw_dir = base_dir / "data" / "raw"
    process_dir = base_dir / "data" / "process"
    
    base_name = "POLIMER_3M2026"
    prep_base_path = process_dir / "preparation" / f"{base_name}_PREPARATION"
    cleansed_base_path = process_dir / "cleansed" / f"{base_name}_CLEANSING"
    learning_base_path = process_dir / "learning" / f"{base_name}_LEARNING"

    # PREPARATION
    logging.info("DATA PREPARATION")
    if not process_and_merge_json(raw_dir, prep_base_path):
        logging.error("Preparation gagal.")
        return

    # CLEANSING
    logging.info("DATA CLEANSING")
    if not apply_cleansing_rules(prep_base_path, cleansed_base_path):
        logging.error("Cleansing gagal.")
        return
    
    # DATA LEARNING
    logging.info("DATA LEARNING")
    if process_learning_data(cleansed_base_path, learning_base_path):
        logging.info("SEMUA PROSES SELESAI")
    else:
        logging.error("Data Learning gagal.")

if __name__ == "__main__":
    main()