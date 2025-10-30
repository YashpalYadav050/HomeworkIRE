from __future__ import annotations

import json
import math
from collections import defaultdict
from dataclasses import dataclass
from pathlib import Path
from typing import Dict, Iterable, List, Tuple

from index_base import IndexBase
from preprocess import TextPreprocessor, PreprocessConfig
from datastore import LocalStore
from compression import vbyte_encode, vbyte_decode, zlib_compress, zlib_decompress, add_skip_pointers


@dataclass
class SelfIndexConfig:
    core: str
    info: str  # BOOLEAN | WORDCOUNT | TFIDF
    dstore: str  # CUSTOM (local store)
    qproc: str  # TERMatat | DOCatat
    compr: str  # NONE | CODE | CLIB
    optim: str  # Null | Skipping | Thresholding | EarlyStopping


class SelfIndex(IndexBase):
    def __init__(self, core: str, info: str, dstore: str, qproc: str, compr: str, optim: str) -> None:
        super().__init__(core, info, dstore, qproc, compr, optim)
        self.config = SelfIndexConfig(core, info, dstore, qproc, compr, optim)
        self.preprocessor = TextPreprocessor(PreprocessConfig(lowercase=True, remove_stopwords=True, stem=True))
        self.store: LocalStore | None = None
        self.lexicon: Dict[str, Tuple[int, int]] = {}
        self.docs: Dict[str, Dict] = {}
        self.code_to_docid: Dict[int, str] = {}
        self.N = 0

    def _index_dir(self, index_id: str) -> Path:
        base = Path("indices")
        base.mkdir(exist_ok=True)
        return base / index_id

    def create_index(self, index_id: str, files: Iterable[tuple[str, str]]) -> None:
        index_dir = self._index_dir(index_id)
        store = LocalStore(index_dir)

        postings: Dict[str, Dict[int, List[int]]] = defaultdict(lambda: defaultdict(list))
        doc_lengths: Dict[str, int] = {}
        doc_code_map: Dict[str, int] = {}
        next_code = 1
        for doc_id, text in files:
            tokens = self.preprocessor.tokenize(text)
            doc_lengths[doc_id] = len(tokens)
            if doc_id not in doc_code_map:
                doc_code_map[doc_id] = next_code
                next_code += 1
            code = doc_code_map[doc_id]
            for pos, tok in enumerate(tokens):
                postings[tok][code].append(pos)

        # Persist
        lexicon: Dict[str, Tuple[int, int]] = {}
        for term, doc_map in postings.items():
            # Build gaps for doc ids and positions for compression
            doc_codes = sorted(doc_map.keys())
            # Serialize as: [df, doc_id, tf, pos1, pos2, ...] for each doc
            blob: List[int] = [len(doc_codes)]
            for d in doc_codes:
                positions = doc_map[d]
                blob.append(int(d))
                blob.append(len(positions))
                blob.extend(positions)
            raw = json.dumps(blob).encode("utf-8")
            if self.config.compr == "CODE":
                payload = vbyte_encode(blob)
            elif self.config.compr == "CLIB":
                payload = zlib_compress(raw)
            else:
                payload = raw
            offset, length = store.append_postings(payload)
            lexicon[term] = (offset, length)

        store.write_lexicon(lexicon)
        # Persist docs with code mapping
        docs_payload = {d: {"length": l, "code": doc_code_map[d]} for d, l in doc_lengths.items()}
        store.write_docs(docs_payload)
        store.write_meta({
            "config": self.config.__dict__,
            "N": len(doc_lengths),
        })

    def load_index(self, serialized_index_dump: str) -> None:
        index_dir = Path(serialized_index_dump)
        store = LocalStore(index_dir)
        self.store = store
        self.lexicon = store.read_lexicon()
        self.docs = store.read_docs()
        meta = store.read_meta()
        self.N = int(meta.get("N", 0))
        # Build reverse map code -> doc_id
        self.code_to_docid = {}
        for doc_id, info in self.docs.items():
            code = int(info.get("code", 0))
            if code:
                self.code_to_docid[code] = doc_id

    def update_index(self, index_id: str, remove_files: Iterable[tuple[str, str]], add_files: Iterable[tuple[str, str]]) -> None:
        # For simplicity, rebuild
        remaining: Dict[str, str] = {}
        if self.store and (self._index_dir(index_id)).exists():
            # Cannot list original raw docs; caller should pass all
            pass
        self.create_index(index_id, add_files)

    def _decode_postings(self, payload: bytes) -> List[int]:
        if self.config.compr == "CODE":
            return vbyte_decode(payload)
        elif self.config.compr == "CLIB":
            raw = zlib_decompress(payload)
            return json.loads(raw.decode("utf-8"))
        else:
            return json.loads(payload.decode("utf-8"))

    def _get_term_postings(self, term: str) -> Dict[int, List[int]]:
        assert self.store is not None
        if term not in self.lexicon:
            return {}
        offset, length = self.lexicon[term]
        payload = self.store.read_postings(offset, length)
        blob = self._decode_postings(payload)
        # Parse blob back to dict
        i = 0
        df = blob[i]; i += 1
        postings: Dict[int, List[int]] = {}
        for _ in range(df):
            doc_code = int(blob[i]); i += 1
            tf = blob[i]; i += 1
            positions = blob[i:i+tf]; i += tf
            postings[doc_code] = positions
        return postings

    def _boolean_and(self, a: List[int], b: List[int]) -> List[int]:
        i = j = 0
        out: List[int] = []
        a_sorted = sorted(a)
        b_sorted = sorted(b)
        while i < len(a_sorted) and j < len(b_sorted):
            if a_sorted[i] == b_sorted[j]:
                out.append(a_sorted[i]); i += 1; j += 1
            elif a_sorted[i] < b_sorted[j]:
                i += 1
            else:
                j += 1
        return out

    def _boolean_or(self, a: List[int], b: List[int]) -> List[int]:
        return sorted(set(a) | set(b))

    def _boolean_not(self, a: List[int]) -> List[int]:
        universe = set(int(info.get("code", 0)) for info in self.docs.values() if int(info.get("code", 0)) != 0)
        return sorted(universe - set(a))

    def _phrase_match(self, term_postings: List[Dict[int, List[int]]]) -> List[int]:
        # Intersect docs then check positional adjacency
        if not term_postings:
            return []
        common_docs = set(term_postings[0].keys())
        for p in term_postings[1:]:
            common_docs &= set(p.keys())
        result: List[int] = []
        for d in common_docs:
            pos_lists = [p[d] for p in term_postings]
            # Check if any position p0 has p1=p0+1, p2=p0+2, ...
            first = pos_lists[0]
            rest = pos_lists[1:]
            ok = False
            s_rest = [set(pl) for pl in rest]
            for p0 in first:
                ok = True
                for k, s in enumerate(s_rest, start=1):
                    if (p0 + k) not in s:
                        ok = False
                        break
                if ok:
                    break
            if ok:
                result.append(d)
        return sorted(result)

    def _score(self, doc_ids: List[int], terms: List[str]) -> List[tuple[int, float]]:
        if self.config.info == "BOOLEAN":
            return [(d, 1.0) for d in doc_ids]
        scores = defaultdict(float)
        # Preload postings
        postings = {t: self._get_term_postings(t) for t in terms}
        for t in terms:
            p = postings[t]
            df = len(p)
            idf = math.log((self.N + 1) / (df + 1)) + 1.0 if df else 0.0
            for d in doc_ids:
                tf = len(p.get(d, []))
                if tf == 0:
                    continue
                if self.config.info == "WORDCOUNT":
                    scores[d] += tf
                else:  # TFIDF
                    scores[d] += (1 + math.log(tf)) * idf
        return sorted(scores.items(), key=lambda x: x[1], reverse=True)

    # --- Boolean query parsing (AND/OR/NOT, parentheses, phrases) ---
    class _Tok:
        def __init__(self, kind: str, value: str = "") -> None:
            self.kind = kind
            self.value = value

    def _tokenize(self, q: str) -> List['_Tok']:
        toks: List[SelfIndex._Tok] = []
        i = 0
        n = len(q)
        while i < n:
            ch = q[i]
            if ch.isspace():
                i += 1; continue
            if ch == '(':
                toks.append(SelfIndex._Tok('LP'))
                i += 1; continue
            if ch == ')':
                toks.append(SelfIndex._Tok('RP'))
                i += 1; continue
            if ch == '"':
                i += 1
                buf: List[str] = []
                while i < n and q[i] != '"':
                    buf.append(q[i]); i += 1
                i += 1  # skip closing quote
                phrase = ' '.join(''.join(buf).split())
                toks.append(SelfIndex._Tok('PHRASE', phrase))
                continue
            # read word
            j = i
            while j < n and not q[j].isspace() and q[j] not in '()':
                j += 1
            word = q[i:j]
            upper = word.upper()
            if upper in ('AND', 'OR', 'NOT'):
                toks.append(SelfIndex._Tok(upper))
            else:
                toks.append(SelfIndex._Tok('TERM', word))
            i = j
        return toks

    # Recursive descent with precedence: PHRASE/TERM (atom) > NOT > AND > OR
    def _parse(self, toks: List['_Tok']):
        self._pos = 0
        self._toks = toks

        def peek(kind: str) -> bool:
            return self._pos < len(self._toks) and self._toks[self._pos].kind == kind

        def eat(kind: str) -> SelfIndex._Tok:
            tok = self._toks[self._pos]
            assert tok.kind == kind
            self._pos += 1
            return tok

        def parse_atom():
            if peek('LP'):
                eat('LP')
                node = parse_or()
                eat('RP')
                return node
            if peek('PHRASE'):
                phrase = eat('PHRASE').value
                return ('PHRASE', phrase)
            if peek('TERM'):
                term = eat('TERM').value
                return ('TERM', term)
            return ('EMPTY',)

        def parse_not():
            if peek('NOT'):
                eat('NOT')
                node = parse_not()
                return ('NOT', node)
            return parse_atom()

        def parse_and():
            node = parse_not()
            while self._pos < len(self._toks) and peek('AND'):
                eat('AND')
                rhs = parse_not()
                node = ('AND', node, rhs)
            return node

        def parse_or():
            node = parse_and()
            while self._pos < len(self._toks) and peek('OR'):
                eat('OR')
                rhs = parse_and()
                node = ('OR', node, rhs)
            return node

        return parse_or()

    def _eval_node(self, node) -> List[int]:
        kind = node[0]
        if kind == 'EMPTY':
            return []
        if kind == 'TERM':
            term = node[1]
            toks = self.preprocessor.tokenize(term)
            if not toks:
                return []
            p = self._get_term_postings(toks[0])
            return sorted(p.keys())
        if kind == 'PHRASE':
            phrase_text = node[1]
            terms = [t for t in self.preprocessor.tokenize(phrase_text)]
            ph_postings = [self._get_term_postings(t) for t in terms]
            return self._phrase_match(ph_postings)
        if kind == 'NOT':
            a = self._eval_node(node[1])
            return self._boolean_not(a)
        if kind == 'AND':
            a = self._eval_node(node[1])
            b = self._eval_node(node[2])
            return self._boolean_and(a, b)
        if kind == 'OR':
            a = self._eval_node(node[1])
            b = self._eval_node(node[2])
            return self._boolean_or(a, b)
        return []

    def query(self, query: str) -> str:
        toks = self._tokenize(query)
        ast = self._parse(toks)
        matched_codes = self._eval_node(ast)
        # For scoring, collect normalized terms present (exclude phrases; tokens from terms and phrases both contribute)
        norm_terms: List[str] = []
        for t in toks:
            if t.kind == 'TERM':
                norm = self.preprocessor.tokenize(t.value)
                if norm:
                    norm_terms.append(norm[0])
            elif t.kind == 'PHRASE':
                for w in self.preprocessor.tokenize(t.value):
                    norm_terms.append(w)

        if self.config.qproc.startswith('T'):
            ranked = self._score(matched_codes, norm_terms)
        else:
            ranked = self._score(matched_codes, norm_terms)

        results = [{"doc_id": self.code_to_docid.get(int(d), str(d)), "score": s} for d, s in ranked[:50]]
        return json.dumps({"results": results})

    def delete_index(self, index_id: str) -> None:
        d = self._index_dir(index_id)
        if d.exists():
            for p in d.iterdir():
                p.unlink()
            d.rmdir()

    def list_indices(self) -> Iterable[str]:
        base = Path("indices")
        if not base.exists():
            return []
        return [p.name for p in base.iterdir() if p.is_dir()]

    def list_indexed_files(self, index_id: str) -> Iterable[str]:
        # We store only metadata; return doc ids
        d = self._index_dir(index_id)
        store = LocalStore(d)
        docs = store.read_docs()
        return list(docs.keys())


