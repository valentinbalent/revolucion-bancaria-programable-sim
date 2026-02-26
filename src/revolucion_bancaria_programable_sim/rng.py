# rng.py
"""
CRN + substreams.

Requirement (F):
- same seed => same demand/shock comparable A/B (shared streams)
- deterministic substreams for risks exclusive to B (world-specific streams)
"""

from __future__ import annotations
import hashlib
from typing import Optional
import numpy as np


def _stable_u32(s: str) -> int:
    h = hashlib.sha256(s.encode("utf-8")).digest()
    return int.from_bytes(h[:4], "little", signed=False)


class CRN:
    """
    Common Random Numbers manager using numpy Generator.

    Streams:
      - shared streams: identical across A/B
      - world streams: incorporate world tag to isolate B-only randomness
    """

    def __init__(self, base_seed: int):
        self.base_seed = int(base_seed)

    def stream(self, name: str, shared: bool = True, world: Optional[str] = None) -> np.random.Generator:
        """
        Create a deterministic generator for (base_seed, name, shared/world).
        """
        tag = f"{self.base_seed}|{name}|{'shared' if shared else 'world'}|{world or ''}"
        spawn = _stable_u32(tag)
        ss = np.random.SeedSequence([self.base_seed, spawn])
        return np.random.Generator(np.random.PCG64(ss))

    def randint(self, name: str, low: int, high: int, shared: bool = True, world: Optional[str] = None) -> int:
        g = self.stream(name, shared=shared, world=world)
        return int(g.integers(low, high))

    def uniform(self, name: str, shared: bool = True, world: Optional[str] = None) -> float:
        g = self.stream(name, shared=shared, world=world)
        return float(g.random())
