import csv
from pathlib import Path
from datetime import datetime
from apps.config import settings


def export_batch(batch: list[dict]):
    filename = f"{settings.output_file_prefix}_{datetime.now().strftime('%Y%m%d')}.csv"
    file_path = Path(settings.output_directory) / filename

    file_exists = file_path.exists()

    with open(file_path, "a", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=batch[0].keys())

        if not file_exists:
            writer.writeheader()

        writer.writerows(batch)