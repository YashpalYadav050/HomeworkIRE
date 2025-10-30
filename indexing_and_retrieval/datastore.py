from __future__ import annotations

import json
from pathlib import Path
from typing import Dict, List, Tuple


class LocalStore:
    """Custom local JSON-based store per index directory.
    Layout:
      meta.json       -> metadata
      terms.jsonl     -> per-term line: {term, df, cf, offset}
      postings.bin    -> contiguous binary blocks of postings payloads
      lexicon.json    -> term -> {offset, length}
      docs.json       -> doc_id -> {length}
    """

    def __init__(self, root: Path) -> None:
        self.root = root
        self.root.mkdir(parents=True, exist_ok=True)
        self.meta_path = self.root / "meta.json"
        self.postings_path = self.root / "postings.bin"
        self.lexicon_path = self.root / "lexicon.json"
        self.docs_path = self.root / "docs.json"

    def write_meta(self, meta: Dict) -> None:
        self.meta_path.write_text(json.dumps(meta))

    def read_meta(self) -> Dict:
        return json.loads(self.meta_path.read_text())

    def write_lexicon(self, lex: Dict[str, Tuple[int, int]]) -> None:
        self.lexicon_path.write_text(json.dumps(lex))

    def read_lexicon(self) -> Dict[str, Tuple[int, int]]:
        return json.loads(self.lexicon_path.read_text())

    def append_postings(self, payload: bytes) -> Tuple[int, int]:
        # returns (offset, length)
        with self.postings_path.open("ab") as f:
            offset = f.tell()
            f.write(payload)
            return offset, len(payload)

    def read_postings(self, offset: int, length: int) -> bytes:
        with self.postings_path.open("rb") as f:
            f.seek(offset)
            return f.read(length)

    def write_docs(self, docs: Dict[str, Dict]) -> None:
        self.docs_path.write_text(json.dumps(docs))

    def read_docs(self) -> Dict[str, Dict]:
        return json.loads(self.docs_path.read_text())


