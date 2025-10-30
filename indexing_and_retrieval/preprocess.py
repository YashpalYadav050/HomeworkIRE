from __future__ import annotations

import re
from collections import Counter
from dataclasses import dataclass
from typing import Iterable, List, Tuple

import nltk


_WORD_RE = re.compile(r"[A-Za-z][A-Za-z\-']+")


def _ensure_nltk() -> None:
    try:
        nltk.data.find("tokenizers/punkt")
    except LookupError:
        nltk.download("punkt", quiet=True)
    try:
        nltk.data.find("corpora/stopwords")
    except LookupError:
        nltk.download("stopwords", quiet=True)


@dataclass
class PreprocessConfig:
    lowercase: bool = True
    remove_stopwords: bool = True
    stem: bool = True


class TextPreprocessor:
    def __init__(self, config: PreprocessConfig | None = None) -> None:
        _ensure_nltk()
        self.config = config or PreprocessConfig()
        self.stopwords = set(nltk.corpus.stopwords.words("english")) if self.config.remove_stopwords else set()
        self.stemmer = nltk.stem.PorterStemmer() if self.config.stem else None

    def tokenize(self, text: str) -> List[str]:
        if self.config.lowercase:
            text = text.lower()
        tokens = _WORD_RE.findall(text)
        if self.stopwords:
            tokens = [t for t in tokens if t not in self.stopwords]
        if self.stemmer is not None:
            tokens = [self.stemmer.stem(t) for t in tokens]
        return tokens

    def term_frequencies(self, docs: Iterable[str]) -> Counter:
        tf: Counter = Counter()
        for doc in docs:
            tf.update(self.tokenize(doc))
        return tf


def top_k_terms(counter: Counter, k: int = 50) -> List[Tuple[str, int]]:
    return counter.most_common(k)


