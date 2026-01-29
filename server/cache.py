from __future__ import annotations

import json
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Optional


@dataclass
class CacheEntry:
    start: str
    end: str
    payload_path: str
    updated_at: float


class DiskCache:
    def __init__(self, cache_dir: Path, max_entries: int = 100) -> None:
        self.cache_dir = cache_dir
        self.max_entries = max_entries
        self.index_path = cache_dir / "index.json"
        self._index: dict[str, CacheEntry] = {}
        self._load_index()

    def _load_index(self) -> None:
        self.cache_dir.mkdir(parents=True, exist_ok=True)
        if not self.index_path.exists():
            self._index = {}
            return
        try:
            raw = json.loads(self.index_path.read_text(encoding="utf-8"))
        except Exception:
            self._index = {}
            return
        index: dict[str, CacheEntry] = {}
        for start, data in raw.items():
            try:
                index[start] = CacheEntry(
                    start=start,
                    end=data["end"],
                    payload_path=data["payload_path"],
                    updated_at=float(data["updated_at"]),
                )
            except Exception:
                continue
        self._index = index

    def _save_index(self) -> None:
        payload = {
            start: {
                "end": entry.end,
                "payload_path": entry.payload_path,
                "updated_at": entry.updated_at,
            }
            for start, entry in self._index.items()
        }
        self.index_path.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")

    def _evict_if_needed(self) -> None:
        if len(self._index) <= self.max_entries:
            return
        ordered = sorted(self._index.values(), key=lambda e: e.updated_at)
        to_remove = ordered[: max(0, len(ordered) - self.max_entries)]
        for entry in to_remove:
            self._remove_entry(entry.start)

    def _remove_entry(self, start: str) -> None:
        entry = self._index.pop(start, None)
        if not entry:
            return
        path = self.cache_dir / entry.payload_path
        try:
            path.unlink(missing_ok=True)
        except Exception:
            pass

    def get(self, start: str, end: str) -> Optional[str]:
        entry = self._index.get(start)
        if not entry:
            return None
        if entry.end != end:
            self._remove_entry(start)
            self._save_index()
            return None
        payload_path = self.cache_dir / entry.payload_path
        if not payload_path.exists():
            self._remove_entry(start)
            self._save_index()
            return None
        entry.updated_at = time.time()
        self._index[start] = entry
        self._save_index()
        return payload_path.read_text(encoding="utf-8")

    def put(self, start: str, end: str, payload_json: str) -> None:
        self._remove_entry(start)
        filename = f"{start.replace(':', '').replace('.', '')}_{end.replace(':', '').replace('.', '')}.json"
        payload_path = self.cache_dir / filename
        payload_path.write_text(payload_json, encoding="utf-8")
        self._index[start] = CacheEntry(
            start=start,
            end=end,
            payload_path=filename,
            updated_at=time.time(),
        )
        self._evict_if_needed()
        self._save_index()
