import csv
import sys
import time
import urllib.request
import urllib.error
import json
from pathlib import Path


API_URL = "https://api.imdbapi.dev/names:batchGet"
BATCH_SIZE = 5
BASE_DELAY = 1.0
MAX_RETRIES = 8
HEADERS = {
    "Accept": "application/json",
    "User-Agent": "Mozilla/5.0 (compatible; imdb-fetcher/1.0)",
}

SOURCES = {
    "cast":     {"file": "data/cast.tsv",     "id_col": "NCONST"},
    "director": {"file": "data/director.tsv", "id_col": "DIRECTORID"},
    "writer":   {"file": "data/writer.tsv",   "id_col": "WRITERID"},
}

FIELDNAMES = [
    "id", "displayName", "birthName", "birthDate",
    "birthLocation", "heightCm", "primaryProfessions",
    "primaryImageUrl", "biography",
]


def fetch_batch(name_ids: list[str]) -> list[dict]:
    params = "&".join(f"nameIds={nid}" for nid in name_ids)
    url = f"{API_URL}?{params}"
    req = urllib.request.Request(url, headers=HEADERS)
    for attempt in range(MAX_RETRIES):
        try:
            with urllib.request.urlopen(req, timeout=30) as resp:
                data = json.loads(resp.read())
            return data.get("names", [])
        except urllib.error.HTTPError as e:
            if e.code == 429:
                retry_after = e.headers.get("Retry-After")
                wait = int(retry_after) if retry_after and retry_after.isdigit() else 2 ** attempt
                print(f"[429 – aguardando {wait}s]", end=" ", flush=True)
                time.sleep(wait)
            else:
                raise
    raise RuntimeError(f"Falhou após {MAX_RETRIES} tentativas (rate limit persistente)")


def flatten_name(name: dict) -> dict:
    bd = name.get("birthDate") or {}
    parts = [bd.get("year"), bd.get("month"), bd.get("day")]
    birth_date = "-".join(str(p).zfill(2) for p in parts if p) if any(parts) else ""
    image = name.get("primaryImage") or {}
    return {
        "id": name.get("id", ""),
        "displayName": name.get("displayName", ""),
        "birthName": name.get("birthName", ""),
        "birthDate": birth_date,
        "birthLocation": name.get("birthLocation", ""),
        "heightCm": name.get("heightCm", ""),
        "primaryProfessions": "|".join(name.get("primaryProfessions") or []),
        "primaryImageUrl": image.get("url", ""),
        "biography": (name.get("biography") or "").replace("\n", " "),
    }


def read_ids(input_path: Path, id_column: str) -> list[str]:
    ids = []
    with open(input_path, newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f, delimiter="\t")
        for row in reader:
            nid = row.get(id_column, "").strip()
            if nid:
                ids.append(nid)
    return ids


def load_checkpoint(checkpoint_path: Path) -> set[str]:
    if not checkpoint_path.exists():
        return set()
    with open(checkpoint_path, encoding="utf-8") as f:
        return {line.strip() for line in f if line.strip()}


def save_checkpoint(checkpoint_path: Path, name_id: str):
    with open(checkpoint_path, "a", encoding="utf-8") as f:
        f.write(name_id + "\n")


def append_output(output_path: Path, records: list[dict]):
    write_header = not output_path.exists()
    with open(output_path, "a", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=FIELDNAMES, delimiter="\t")
        if write_header:
            writer.writeheader()
        writer.writerows(records)


def main():
    if len(sys.argv) < 2 or sys.argv[1] not in SOURCES:
        print(f"Uso: python3 main.py <tipo> [limite]")
        print(f"     tipo:   {' | '.join(SOURCES)}")
        print(f"     limite: número de linhas a processar (padrão: todas)")
        sys.exit(1)

    source_key = sys.argv[1]
    limit = int(sys.argv[2]) if len(sys.argv) > 2 else None

    source = SOURCES[source_key]
    input_path = Path(source["file"])
    id_column = source["id_col"]
    output_path = Path(f"data/output_{source_key}.tsv")
    checkpoint_path = output_path.with_suffix(".checkpoint")

    print(f"Fonte: '{source_key}' | Arquivo: '{input_path}' | Coluna ID: '{id_column}'")

    all_ids = read_ids(input_path, id_column)
    done = load_checkpoint(checkpoint_path)

    pending = [nid for nid in all_ids if nid not in done]
    if limit is not None:
        # Respeita o limite considerando o que já foi feito
        already_done_in_range = sum(1 for nid in all_ids[:limit] if nid in done)
        remaining_limit = limit - already_done_in_range
        pending = pending[:remaining_limit]

    if done:
        print(f"{len(done)} IDs já processados (retomando checkpoint).")
    print(f"{len(pending)} IDs a processar" + (f" (limite: {limit})" if limit else "") + ".")

    if not pending:
        print("Nada a fazer.")
        return

    total_saved = 0
    for i in range(0, len(pending), BATCH_SIZE):
        batch = pending[i : i + BATCH_SIZE]
        global_pos = len(done) + i + 1
        global_end = len(done) + i + len(batch)
        print(f"  [{global_pos}–{global_end}/{len(all_ids)}]", end=" ", flush=True)
        try:
            names = fetch_batch(batch)
            records = [flatten_name(n) for n in names]
            append_output(output_path, records)
            for nid in batch:
                save_checkpoint(checkpoint_path, nid)
            total_saved += len(records)
            print(f"{len(records)} salvos.")
        except Exception as e:
            print(f"ERRO: {e}")

        if i + BATCH_SIZE < len(pending):
            time.sleep(BASE_DELAY)

    all_done = load_checkpoint(checkpoint_path)
    print(f"\nConcluído. {total_saved} registros salvos em '{output_path}'.")
    if len(all_done) >= len(all_ids):
        checkpoint_path.unlink()
        print("Checkpoint removido (todos os IDs processados).")


if __name__ == "__main__":
    main()
