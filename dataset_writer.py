import json
from datetime import datetime
import os

DATASET_DIR = "datasets"
os.makedirs(DATASET_DIR, exist_ok=True)

VALIDATION_FILE = os.path.join(DATASET_DIR, "validation_dataset.jsonl")
ANALYSIS_FILE = os.path.join(DATASET_DIR, "analysis_dataset.jsonl")

def save_validation_sample(pdf, section, message):
    record = {
        "pdf": os.path.basename(pdf),
        "section": section,
        "text": message,
        "label": message,
        "created_at": datetime.utcnow().isoformat()
    }
    with open(VALIDATION_FILE, "a", encoding="utf-8") as f:
        f.write(json.dumps(record) + "\n")

def save_analysis_sample(pdf, prompt, output):
    record = {
        "instruction": prompt,
        "input": f"PDF: {os.path.basename(pdf)}",
        "output": output
    }
    with open(ANALYSIS_FILE, "a", encoding="utf-8") as f:
        f.write(json.dumps(record) + "\n")
