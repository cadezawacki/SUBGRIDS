

try:
    import re2 as re
except ImportError:
    import re

from collections import deque, defaultdict
import aiologic
import asyncio
from typing import Any, Callable, Optional, Set, Dict, List, Tuple, Pattern, Final

import polars as pl
from app.services.payload.columnar_codec import OptimizedColumnarCodec
from app.logs.logging import log
from app.services.payload.payloadV4 import BroadcastMessage, Payloads, Publish

# ---------------- Constants / aliases ----------------

MAX_PREVIEW_SUBS: Final = 5
DEDUPE_SUBSCRIBERS_DEFAULT: Final = False  # keep backward-compat output shape for publish/publishDelta
Subscription = Tuple[Any, Optional[Callable[[Any], bool]], str]  # (subscriber, filter, original_pattern)
LocationID = object  # opaque pointer: node object or pattern key
_CODEC = OptimizedColumnarCodec()

# ---------------- Internal helpers ----------------

def _norm_topic(s: str) -> str:
    """Router-wide canonicalization for topics/patterns (case-insensitive)."""
    return s.upper()

# ---------------- Internal nodes ----------------

class _TrieNode:
    __slots__ = ("children", "exact", "hash_subs")
    def __init__(self):
        self.children: Dict[str, _TrieNode] = {}
        self.exact: Dict[Any, Tuple[Optional[Callable], str]] = {}       # subscriber -> (filter, pattern)
        self.hash_subs: Dict[Any, Tuple[Optional[Callable], str]] = {}   # 'a.#' subscriptions at this prefix

    def is_empty(self) -> bool:
        return not self.children and not self.exact and not self.hash_subs

class _StarNode:
    __slots__ = ("children", "star", "subs")
    def __init__(self):
        self.children: Dict[str, _StarNode] = {}
        self.star: Optional[_StarNode] = None       # child for '*'
        self.subs: Dict[Any, Tuple[Optional[Callable], str]] = {}  # subs that end exactly here

    def is_empty(self) -> bool:
        return not self.children and self.star is None and not self.subs

class _SuffixNode:
    __slots__ = ("children", "subs")
    def __init__(self):
        self.children: Dict[str, _SuffixNode] = {}  # char -> node (reversed)
        self.subs: Dict[Any, Tuple[Optional[Callable], str]] = {}  # subs for this complete suffix

    def is_empty(self) -> bool:
        return not self.children and not self.subs

# ---------------- Router ----------------

class PubSubRouter:
    # Removed functions from original:
    #   - _find_subscription_tuple (replaced by dicts -> O(1))
    #   - _remove_from_regex_list (replaced by dict mapping)
    def __init__(self, delimiter: str = '.', debug=False):
        if not isinstance(delimiter, str) or len(delimiter) != 1:
            raise ValueError("Delimiter must be a single character string.")
        self.delimiter: str = delimiter
        self._escaped_delim = re.escape(delimiter)

        # Prefix trie for exact + '#'
        self._root: _TrieNode = _TrieNode()

        # Token trie for '*' patterns (no '%', no '#')
        self._star_root: _StarNode = _StarNode()

        # Reversed-suffix trie for leading-percent patterns like '%.META' (no other wildcards)
        self._suffix_root: _SuffixNode = _SuffixNode()

        # Global '%' alone (match-any) bucket
        self._match_all: Dict[Any, Tuple[Optional[Callable], str]] = {}

        # Regex fallback: pattern_str -> (compiled_regex, subs_dict)
        self._regex_bucket: Dict[str, Tuple[Pattern, Dict[Any, Tuple[Optional[Callable], str]]]] = {}

        # Subscriber locations: subscriber -> set of (pattern, location_id)
        # location_id is the exact node or the pattern_str (regex) used for fast unsubscribe
        self._subscriber_locations = defaultdict(set)
        self.debug = debug
        self.sub_locks = defaultdict(lambda: asyncio.Lock())

        # Validation: allow alnum + delimiter + wildcards (#,*,%)
        self._validation_re: Pattern = re.compile(r"^[a-zA-Z0-9" + self._escaped_delim + r"#%*]+$")

        # Optional codec for publishDelta
        self._codec = _CODEC

    # ---------------- Utilities ----------------
    async def shutdown(self):
        self._subscriber_locations.clear()
        self._root = None
        self._suffix_root = None
        self._match_all.clear()
        self.sub_locks.clear()
        self._codec = None

    async def get_lock(self, subscriber):
        lock = self.sub_locks[subscriber]
        await lock.acquire()
        return lock

    async def _add_location(self, subscriber: Any, pattern: str, location: LocationID):
        lock = await self.get_lock(subscriber)
        try:
            locs = self._subscriber_locations[subscriber]
            locs.add((_norm_topic(pattern), location))
        finally:
            lock.release()

    async def _remove_location(self, subscriber: Any, pattern: str, location: LocationID):
        lock = await self.get_lock(subscriber)
        try:
            locs = self._subscriber_locations[subscriber]
            locs.discard((_norm_topic(pattern), location))
            if not locs:
                self._subscriber_locations.pop(subscriber, None)
        finally:
            lock.release()

    def _validate_pattern(self, pattern: str) -> None:
        if not isinstance(pattern, str) or not pattern:
            raise ValueError("Topic pattern must be a non-empty string.")
        pattern_uc = _norm_topic(pattern)
        if not self._validation_re.match(pattern_uc):
            raise ValueError(f"Pattern '{pattern}' contains invalid characters.")
        if self.delimiter * 2 in pattern_uc:
            raise ValueError(f"Pattern '{pattern}' contains invalid adjacent delimiters.")
        if pattern_uc != '#' and (pattern_uc.startswith(self.delimiter) or (pattern_uc.endswith(self.delimiter) and not pattern_uc.endswith(self.delimiter + '#'))):
            raise ValueError(f"Pattern '{pattern}' has invalid leading/trailing delimiters (except trailing '#').")
        if '#' in pattern_uc and ('*' in pattern_uc or '%' in pattern_uc):
            raise ValueError(f"Cannot mix '#' with '*' or '%' in '{pattern}'")
        segs = pattern_uc.split(self.delimiter)
        if "" in segs[:-1] or (segs[-1] == "" and pattern_uc[-1] != '#'):
            raise ValueError(f"Pattern '{pattern}' contains empty segments.")

    def _classify(self, pattern: str) -> str:
        # 'hash' -> '#' present; 'exact' -> no wildcards; 'suffix' -> leading '%' only with the rest literal;
        # 'star' -> only '*' wildcards; 'regex' -> everything else.
        pattern_uc = _norm_topic(pattern)
        if '#' in pattern_uc:
            return "hash"
        if '*' not in pattern_uc and '%' not in pattern_uc:
            return "exact"
        if pattern_uc == '%':
            return "match_all"
        if '%' in pattern_uc:
            segs = pattern_uc.split(self.delimiter)
            if segs[0] == '%' and '*' not in pattern_uc and segs.count('%') == 1:
                # ensure remaining segments are literal
                for s in segs[1:]:
                    if ('%' in s) or ('*' in s):
                        return "regex"
                return "suffix"
            return "regex"
        # only '*' present
        return "star"

    def _compile_regex(self, pattern: str) -> Pattern:
        pattern_uc = _norm_topic(pattern)
        segs = pattern_uc.split(self.delimiter)
        if len(segs) == 1 and segs[0] == '*':
            return re.compile(f'^[^{self._escaped_delim}]+$')
        out = []
        for seg in segs:
            if seg == '%':
                out.append('.*')
            elif seg == '*':
                out.append(f"[^{self._escaped_delim}]+")
            elif ('%' in seg) or ('*' in seg):
                raise ValueError(f"Wildcards must be full segments: '{seg}' in '{pattern}'")
            else:
                out.append(re.escape(seg))
        return re.compile("^" + self._escaped_delim.join(out) + "$")

    def _format_sub_list(self, subs_dict: Dict[Any, Tuple[Optional[Callable], str]], max_items: int = MAX_PREVIEW_SUBS) -> str:
        if not subs_dict:
            return ""
        names = sorted(repr(s) for s in subs_dict.keys())
        n = len(names)
        if n > max_items:
            return f"[{', '.join(names[:max_items])}, ... ({n - max_items} more)]"
        return f"[{', '.join(names)}]"

    # ---------------- Trie helpers ----------------

    def _trie_get_or_create(self, topic_pattern: str) -> _TrieNode:
        node = self._root
        topic_pattern_uc = _norm_topic(topic_pattern)
        if topic_pattern_uc == '#': return node
        parts = topic_pattern_uc.split(self.delimiter)
        if topic_pattern_uc.endswith(self.delimiter + '#'):
            parts = parts[:-1]  # drop trailing '#'
        for p in parts:
            nxt = node.children.get(p)
            if nxt is None:
                nxt = _TrieNode()
                node.children[p] = nxt
            node = nxt
        return node

    def _trie_find(self, topic_pattern: str) -> Optional[_TrieNode]:
        node = self._root
        topic_pattern_uc = _norm_topic(topic_pattern)
        if topic_pattern_uc == '#': return node
        parts = topic_pattern_uc.split(self.delimiter)
        if topic_pattern_uc.endswith(self.delimiter + '#'):
            parts = parts[:-1]
        for p in parts:
            node = node.children.get(p)
            if node is None:
                return None
        return node

    # ---------------- Star-trie helpers ----------------

    async def _star_insert(self, pattern: str, subscriber: Any, filt: Optional[Callable]):
        async with self.sub_locks[subscriber]:
            node = self._star_root
            pattern_uc = _norm_topic(pattern)
            for seg in pattern_uc.split(self.delimiter):
                if seg == '*':
                    if node.star is None:
                        node.star = _StarNode()
                    node = node.star
                else:
                    nxt = node.children.get(seg)
                    if nxt is None:
                        nxt = _StarNode()
                        node.children[seg] = nxt
                    node = nxt
            node.subs[subscriber] = (filt, pattern_uc)

    async def _star_remove(self, pattern: str, subscriber: Any) -> bool:
        async with self.sub_locks[subscriber]:
            # Iterative remove with path stack for pruning
            stack: List[Tuple[_StarNode, str]] = []
            node = self._star_root
            pattern_uc = _norm_topic(pattern)
            for seg in pattern_uc.split(self.delimiter):
                stack.append((node, seg))
                if seg == '*':
                    node = node.star
                else:
                    node = node.children.get(seg)
                if node is None:
                    return False
            removed = subscriber in node.subs
            if removed:
                node.subs.pop(subscriber, None)
            # prune empty nodes upwards
            for i in range(len(stack) - 1, -1, -1):
                parent, seg = stack[i]
                if seg == '*':
                    child = parent.star
                    if child and child.is_empty():
                        parent.star = None
                else:
                    child = parent.children.get(seg)
                    if child and child.is_empty():
                        parent.children.pop(seg, None)
            return removed

    def _star_match(self, segments: List[str], out: Dict[Any, Tuple[Optional[Callable], str]]):
        # BFS over states to avoid recursion; avoid enqueuing duplicate nodes
        nodes = [self._star_root]
        for seg in segments:
            if not nodes:
                return
            new_nodes: List[_StarNode] = []
            seen_ids = set()
            for nd in nodes:
                ch = nd.children.get(seg)
                if ch is not None:
                    if id(ch) not in seen_ids:
                        seen_ids.add(id(ch))
                        new_nodes.append(ch)
                st = nd.star
                if st is not None:
                    if id(st) not in seen_ids:
                        seen_ids.add(id(st))
                        new_nodes.append(st)
            nodes = new_nodes
        if not nodes:
            return
        for nd in nodes:
            if nd.subs:
                out.update(nd.subs)

    # ---------------- Suffix-trie helpers ----------------

    def _suffix_key_from_pattern(self, pattern: str) -> str:
        # pattern: leading '%' only, rest literal; suffix must include the delimiter between literal parts.
        pattern_uc = _norm_topic(pattern)
        parts = pattern_uc.split(self.delimiter)[1:]  # drop '%'
        if not parts:
            return ""  # shouldn't happen; '%' handled separately
        # Include the leading delimiter so '%.META' does not match topic 'META'
        return self.delimiter + self.delimiter.join(parts)

    async def _suffix_insert(self, suffix: str, subscriber: Any, filt: Optional[Callable], pattern: str):
        async with self.sub_locks[subscriber]:
            node = self._suffix_root
            for ch in reversed(suffix):
                nxt = node.children.get(ch)
                if nxt is None:
                    nxt = _SuffixNode()
                    node.children[ch] = nxt
                node = nxt
            node.subs[subscriber] = (filt, _norm_topic(pattern))

    async def _suffix_remove(self, suffix: str, subscriber: Any) -> bool:
        async with self.sub_locks[subscriber]:
            stack: List[Tuple[_SuffixNode, str]] = []
            node = self._suffix_root
            for ch in reversed(suffix):
                stack.append((node, ch))
                node = node.children.get(ch)
                if node is None:
                    return False
            removed = subscriber in node.subs
            if removed:
                node.subs.pop(subscriber, None)
            # prune
            for i in range(len(stack) - 1, -1, -1):
                parent, ch = stack[i]
                child = parent.children.get(ch)
                if child and child.is_empty():
                    parent.children.pop(ch, None)
            return removed

    def _suffix_match(self, topic_uc: str, out: Dict[Any, Tuple[Optional[Callable], str]]):
        node = self._suffix_root
        for ch in reversed(topic_uc):
            node = node.children.get(ch)
            if node is None:
                return
            if node.subs:
                out.update(node.subs)

    # ---------------- Public API ----------------

    async def subscribe(self, subscriber: Any, topic_pattern: str, filter_callable=None):

        self._validate_pattern(topic_pattern)
        topic_pattern_uc = _norm_topic(topic_pattern)

        # Late-binding for json-like filters in your stack (kept for compatibility)
        if isinstance(filter_callable, (str, dict, bytes)):
            from app.services.rules.filters import create_filter_callable_from_json
            filter_callable = create_filter_callable_from_json(filter_callable)

        kind = self._classify(topic_pattern_uc)

        if kind == "exact" or kind == "hash":
            node = self._trie_get_or_create(topic_pattern_uc)
            is_hash = (topic_pattern_uc == '#' or topic_pattern_uc.endswith(self.delimiter + '#'))
            bucket = node.hash_subs if is_hash else node.exact
            if subscriber not in bucket:
                bucket[subscriber] = (filter_callable, topic_pattern_uc)
                await self._add_location(subscriber, topic_pattern_uc, node)
            return True

        if kind == "match_all":
            if subscriber not in self._match_all:
                self._match_all[subscriber] = (filter_callable, topic_pattern_uc)
                await self._add_location(subscriber, topic_pattern_uc, self._match_all)
            return True

        if kind == "star":
            await self._star_insert(topic_pattern_uc, subscriber, filter_callable)
            await self._add_location(subscriber, topic_pattern_uc, self._star_root)
            return True

        if kind == "suffix":
            suffix = self._suffix_key_from_pattern(topic_pattern_uc)
            await self._suffix_insert(suffix, subscriber, filter_callable, topic_pattern_uc)
            await self._add_location(subscriber, topic_pattern_uc, (self._suffix_root, suffix))
            return True

        # fallback: regex bucket
        entry = self._regex_bucket.get(topic_pattern_uc)
        if entry is None:
            cre = self._compile_regex(topic_pattern_uc)
            subs: Dict[Any, Tuple[Optional[Callable], str]] = {}
            self._regex_bucket[topic_pattern_uc] = (cre, subs)
            entry = (cre, subs)
        if subscriber not in entry[1]:
            entry[1][subscriber] = (filter_callable, topic_pattern_uc)
            await self._add_location(subscriber, topic_pattern_uc, topic_pattern_uc)
            return True
        return True

    async def update_filter(self, subscriber: Any, topic_pattern: str, filter_callable_or_json) -> bool:
        topic_pattern_uc = _norm_topic(topic_pattern)
        if isinstance(filter_callable_or_json, (str, dict, bytes)):
            from app.services.rules.filters import create_filter_callable_from_json
            filt = create_filter_callable_from_json(filter_callable_or_json)
        else:
            filt = filter_callable_or_json

        # Fast path from location map
        locs = self._subscriber_locations[subscriber]
        if not locs:
            return await self.subscribe(subscriber, topic_pattern_uc, filt)

        found = False
        for p, lid in tuple(locs):
            if p != topic_pattern_uc: continue
            found = True
            # exact/hash

            if isinstance(lid, _TrieNode):
                node = lid
                is_hash = (topic_pattern_uc == '#' or topic_pattern_uc.endswith(self.delimiter + '#'))
                bucket = node.hash_subs if is_hash else node.exact
                v = bucket.get(subscriber)
                if v is None: return False
                bucket[subscriber] = (filt, v[1])
                return True

            # match-all dict
            if lid is self._match_all:
                v = self._match_all.get(subscriber)
                if v is None: return False
                self._match_all[subscriber] = (filt, v[1])
                return True

            # star root
            if isinstance(lid, _StarNode):

                if not (await self._star_remove(topic_pattern_uc, subscriber)):
                    return False

                # Reinsert via remove/insert to avoid costly search
                await self._star_insert(topic_pattern_uc, subscriber, filt)
                await self._remove_location(subscriber, topic_pattern_uc, lid)
                await self._add_location(subscriber, topic_pattern_uc, lid)
                return True

            # suffix tuple
            if isinstance(lid, tuple) and (len(lid) == 2) and isinstance(lid[0], _SuffixNode):
                root, suff = lid
                if not (await self._suffix_remove(suff, subscriber)): return False
                await self._suffix_insert(suff, subscriber, filt, topic_pattern_uc)
                await self._remove_location(subscriber, topic_pattern_uc, lid)
                await self._add_location(subscriber, topic_pattern_uc, (root, suff))
                return True

            # regex
            async with self.sub_locks[subscriber]:
                entry = self._regex_bucket.get(str(lid))
                if not entry: return False
                subs = entry[1]
                v = subs.get(subscriber)
                if v is None: return False
                subs[subscriber] = (filt, v[1])
            return True
        if not found:
            return await self.subscribe(subscriber, topic_pattern_uc, filt)
        return False

    def _trie_prune_after_unsub(self, topic_pattern: str):
        topic_pattern_uc = _norm_topic(topic_pattern)
        if topic_pattern_uc == '#': return
        parts = topic_pattern_uc.split(self.delimiter)
        if topic_pattern_uc.endswith(self.delimiter + '#'):
            parts = parts[:-1]
        if not parts: return
        stack = []
        node = self._root
        for seg in parts:
            stack.append((node, seg))
            node = node.children.get(seg)
            if node is None: return
        # Walk back up, removing empty nodes
        cur = node
        for parent, seg in reversed(stack):
            if cur.is_empty():
                parent.children.pop(seg, None)
                cur = parent
            else:
                break

    async def unsubscribe(self, subscriber: Any, topic_pattern: str) -> bool:
        if not isinstance(topic_pattern, str) or not topic_pattern:
            return False
        topic_pattern_uc = _norm_topic(topic_pattern)
        locs = self._subscriber_locations[subscriber]
        target_lid = None
        for p, lid in locs:
            if p == topic_pattern_uc:
                target_lid = lid
                break
        if target_lid is None:
            return False

        removed = False
        lid = target_lid

        if isinstance(lid, _TrieNode):
            node = lid
            is_hash = (topic_pattern_uc == '#' or topic_pattern_uc.endswith(self.delimiter + '#'))
            bucket = node.hash_subs if is_hash else node.exact
            removed = bucket.pop(subscriber, None) is not None
            if removed:
                self._trie_prune_after_unsub(topic_pattern_uc)

        elif lid is self._match_all:
            async with self.sub_locks[subscriber]:
                removed = self._match_all.pop(subscriber, None) is not None

        elif isinstance(lid, _StarNode):
            removed = await self._star_remove(topic_pattern_uc, subscriber)

        elif isinstance(lid, tuple) and len(lid) == 2 and isinstance(lid[0], _SuffixNode):
            # lid = (suffix_root, suffix_str)
            removed = await self._suffix_remove(lid[1], subscriber)

        else:
            async with self.sub_locks[subscriber]:
                entry = self._regex_bucket.get(str(lid))
                if entry:
                    subs = entry[1]
                    removed = subs.pop(subscriber, None) is not None
                    if removed and not subs:
                        self._regex_bucket.pop(str(lid), None)

        if removed:
            await self._remove_location(subscriber, topic_pattern_uc, target_lid)
        return removed

    async def unsubscribe_all(self, subscriber: Any) -> int:
        locs = list(self._subscriber_locations[subscriber])
        count = 0
        for pattern, _ in locs:
            if await self.unsubscribe(subscriber, pattern):
                count += 1
        async with self.sub_locks[subscriber]:
            self._subscriber_locations.pop(subscriber, None)
        return count

    # ---------------- Matching ----------------

    def _collect_potential(self, topic: str) -> Dict[Any, Tuple[Optional[Callable], str]]:
        """
        Returns mapping subscriber -> (filter, original_pattern) for a case-insensitive topic.
        """
        topic_uc = _norm_topic(topic)
        out: Dict[Any, Tuple[Optional[Callable], str]] = {}

        # match-all '%'
        if self._match_all:
            out.update(self._match_all)

        # prefix trie: root hash first
        node = self._root
        if node.hash_subs:
            out.update(node.hash_subs)
        parts = topic_uc.split(self.delimiter)
        for seg in parts:
            node = node.children.get(seg) if node else None
            if node is None:
                break
            if node.hash_subs:
                out.update(node.hash_subs)
        if node is not None and node.exact:
            out.update(node.exact)

        # star-trie
        if self._star_root.children or self._star_root.star or self._star_root.subs:
            self._star_match(parts, out)

        # suffix-trie
        if self._suffix_root.children or self._suffix_root.subs:
            self._suffix_match(topic_uc, out)

        # regex fallback
        if self._regex_bucket:
            # All regexes compiled against UPPER topics; we match once against topic_uc
            for cre, subs in self._regex_bucket.values():
                if cre.match(topic_uc):
                    out.update(subs)

        return out

    def _compile_potentials(self, potentials, dedupe_subscribers=True):
        seen = set()
        out: Set[Tuple[Any, str]] = set()
        if isinstance(potentials, dict):
            potentials = list(potentials.items())
        for s, (_, pat) in potentials:
            if (not dedupe_subscribers) or (s not in seen):
                seen.add(s)
                out.add((s, pat))
        return out

    def publish(self, topic: str, payload, strict: bool = False, dedupe_subscribers: bool = DEDUPE_SUBSCRIBERS_DEFAULT):
        if strict and any(w in topic for w in ('#', '*', '%')):
            raise ValueError(f"Cannot publish to a topic containing wildcards when strict is True: '{topic}'")

        try:
            if isinstance(payload, Publish):
                pld = payload.data.payloads
                if pld is not None:
                    return self.publishDelta(topic, pld, dedupe_subscribers=dedupe_subscribers)
            if isinstance(payload, dict):
                data = payload.get("data") or {}
                pld = data.get("payloads")
                if pld is not None:
                    return self.publishDelta(topic, pld, dedupe_subscribers=dedupe_subscribers)
        except Exception as e:
            log.error(f"Error during publish: {e}")

        potentials = self._collect_potential(topic)
        return self._compile_potentials(potentials, dedupe_subscribers=dedupe_subscribers)

    def publishDelta(self, topic: str, payloads, dedupe_subscribers: bool = DEDUPE_SUBSCRIBERS_DEFAULT):
        potentials = self._collect_potential(topic)
        # log.notify(f'Potential Recipients: {len(potentials)}')
        if not potentials:
            return set()

        no_filter: List[Tuple[Any, str]] = []
        with_filter: List[Tuple[Any, Callable, str]] = []
        for s, (f, pat) in potentials.items():
            (no_filter if f is None else with_filter).append((s, f, pat) if f is not None else (s, pat))

        # Accept multiple payload carrier shapes; iterate rows lazily
        def _iter_rows_variant(v):
            if v is None:
                return
            if hasattr(v, 'frame'):
                yield from _iter_rows_variant(v.frame)
                return
            if pl and isinstance(v, pl.DataFrame):
                for row in v.iter_rows(named=True):
                    yield row
                return
            if isinstance(v, dict) and "frame" in v:
                yield from _iter_rows_variant(v["frame"])
                return
            if isinstance(v, dict) and "_format" in v and self._codec:
                for df in self._codec.decode_to_polars_partitions(v):
                    for row in df.iter_rows(named=True):
                        yield row
                return
            if isinstance(v, list):
                for item in v:
                    yield from _iter_rows_variant(item)
                return
            if isinstance(v, dict):
                yield v

        add = payloads.add if isinstance(payloads, Payloads) else payloads.get("add", None)
        upd = payloads.update if isinstance(payloads, Payloads) else payloads.get("update", None)
        rem = payloads.remove if isinstance(payloads, Payloads) else payloads.get("remove", None)

        def _rows():
            if add is not None:  yield from _iter_rows_variant(add)
            if upd is not None:  yield from _iter_rows_variant(upd)
            if rem is not None:  yield from _iter_rows_variant(rem)

        matched_subs: Set[Any] = set()
        if with_filter:
            unmatched_needed: Set[Any] = {s for (s, _f, _pat) in with_filter}
            # Evaluate each filter until it first matches some row
            for row in _rows():
                if not unmatched_needed:
                    break
                # Iterate over a snapshot to avoid churn on removal
                for s, f, _pat in tuple(with_filter):
                    if s not in unmatched_needed:
                        continue
                    try:
                        if f(row):
                            matched_subs.add(s)
                            unmatched_needed.discard(s)
                    except Exception as e:
                        log.warning(str(e))
                        # Fail-open keeps backward compat behavior on filter error
                        matched_subs.add(s)
                        unmatched_needed.discard(s)

        # Output assembly: keep legacy shapes and dedupe semantics
        seen: Set[Any] = set()
        out: Set[Tuple[Any, str]] = set()

        for s, pat in no_filter:
            if s not in seen:
                seen.add(s); out.add((s, pat))
        for s, _f, pat in with_filter:
            if (s in matched_subs) and (s not in seen):
                seen.add(s); out.add((s, pat))

        return out if dedupe_subscribers else out  # shape preserved; dedupe handled as above

    # ---------------- Introspection ----------------

    def _count_trie_nodes(self, node: _TrieNode) -> int:
        n = 1
        for c in node.children.values():
            n += self._count_trie_nodes(c)
        return n

    def get_all_subscribers_for_topic(self, topic: str):
        # Backward-compatible: returns {(subscriber, topic)} like original
        pot = self._collect_potential(topic)
        return {(s, topic) for s in pot.keys()}

    def get_exact_subscribers_for_topic(self, topic: str):
        # Returns {(subscriber, topic)} only for exact (no wildcards)
        node = self._root
        for seg in _norm_topic(topic).split(self.delimiter):
            node = node.children.get(seg)
            if node is None:
                return set()
        return {(s, topic) for s in node.exact.keys()}

    def count_subscribers_for_topic(self, topic: str) -> int:
        return len(self._collect_potential(topic))

    def count_exact_subscribers_for_topic(self, topic: str) -> int:
        node = self._root
        for seg in _norm_topic(topic).split(self.delimiter):
            node = node.children.get(seg)
            if node is None:
                return 0
        return len(node.exact)

    def get_stats(self) -> Dict[str, Any]:
        # summary
        total_unique_subscribers = len(self._subscriber_locations)
        total_individual_subscriptions = sum(len(locs) for locs in self._subscriber_locations.values())

        # trie
        trie_node_count = self._count_trie_nodes(self._root)
        trie_subscription_count = 0
        q = [self._root]; seen = {id(self._root)}
        while q:
            nd = q.pop()
            trie_subscription_count += len(nd.exact) + len(nd.hash_subs)
            for ch in nd.children.values():
                if id(ch) not in seen:
                    seen.add(id(ch)); q.append(ch)

        # star-trie count
        def _star_count(n: _StarNode) -> Tuple[int, int]:
            nodes = 1
            subs = len(n.subs)
            if n.star:
                a, b = _star_count(n.star)
                nodes += a; subs += b
            for c in n.children.values():
                a, b = _star_count(c)
                nodes += a; subs += b
            return nodes, subs
        star_nodes, star_subs = _star_count(self._star_root)

        # suffix-trie count
        def _suf_count(n: _SuffixNode) -> Tuple[int, int]:
            nodes = 1
            subs = len(n.subs)
            for c in n.children.values():
                a, b = _suf_count(c)
                nodes += a; subs += b
            return nodes, subs
        suf_nodes, suf_subs = _suf_count(self._suffix_root)

        # regex bucket count
        regex_pattern_count = len(self._regex_bucket)
        regex_subscription_count = sum(len(subs) for (_cre, subs) in self._regex_bucket.values())

        # details
        subscriptions_per_subscriber: Dict[str, List[str]] = {}
        for sub, locs in self._subscriber_locations.items():
            subscriptions_per_subscriber[repr(sub)] = sorted([p for (p, _lid) in locs])

        subscribers_per_pattern: Dict[str, List[str]] = {}
        for sub, locs in self._subscriber_locations.items():
            srepr = repr(sub)
            for p, _ in locs:
                subscribers_per_pattern.setdefault(p, []).append(srepr)
        for p in subscribers_per_pattern:
            subscribers_per_pattern[p].sort()

        subscription_counts_per_subscriber = {repr(sub): len(locs) for sub, locs in self._subscriber_locations.items()}

        return {
            "summary": {
                "total_unique_subscribers": total_unique_subscribers,
                "total_individual_subscriptions": total_individual_subscriptions,
            },
            "trie": {
                "node_count": trie_node_count,
                "subscription_count": trie_subscription_count,
            },
            "star_trie": {
                "node_count": star_nodes,
                "subscription_count": star_subs,
            },
            "suffix_trie": {
                "node_count": suf_nodes,
                "subscription_count": suf_subs,
            },
            "regex": {
                "pattern_count": regex_pattern_count,
                "subscription_count": regex_subscription_count,
                "patterns": sorted(self._regex_bucket.keys()),
            },
            "details": {
                "subscriptions_per_subscriber": subscriptions_per_subscriber,
                "subscribers_per_pattern": subscribers_per_pattern,
                "subscription_counts_per_subscriber": subscription_counts_per_subscriber,
            },
        }

    def subscriptions_for(self, subscriber: Any) -> List[str]:
        locs = self._subscriber_locations[subscriber]
        return sorted({pattern for (pattern, _loc) in locs})

    # ---------------- Pretty printing ----------------

    def _build_trie_str(self, node: _TrieNode, prefix: str, is_last: bool, segment: str) -> List[str]:
        lines = []
        connector = "└── " if is_last else "├── "
        parts = []
        if node.exact:
            parts.append(f"E: {self._format_sub_list(node.exact, MAX_PREVIEW_SUBS)}")
        if node.hash_subs:
            parts.append(f"#: {self._format_sub_list(node.hash_subs, MAX_PREVIEW_SUBS)}")
        line = prefix + connector + segment + ("" if not parts else " (" + ", ".join(parts) + ")")
        lines.append(line)
        child_prefix = prefix + ("    " if is_last else "│   ")
        keys = sorted(node.children.keys())
        for i, k in enumerate(keys):
            lines.extend(self._build_trie_str(node.children[k], child_prefix, i == len(keys) - 1, k))
        return lines

    def __str__(self) -> str:
        out = [f"PubSubRouter (delimiter='{self.delimiter}')", "--- Prefix Trie ---"]
        root_parts = []
        if self._root.exact:
            root_parts.append(f"E: {self._format_sub_list(self._root.exact)}")
        if self._root.hash_subs:
            root_parts.append(f"#: {self._format_sub_list(self._root.hash_subs)}")
        out.append("[root]" + ("" if not root_parts else " (" + ", ".join(root_parts) + ")"))
        keys = sorted(self._root.children.keys())
        for i, k in enumerate(keys):
            out.extend(self._build_trie_str(self._root.children[k], "", i == len(keys) - 1, k))
        if not keys and not root_parts:
            out[-1] += " (empty)"

        out.append("\n--- Star Patterns ('*') ---")
        out.append("(None)" if not (self._star_root.children or self._star_root.star or self._star_root.subs) else "(present)")

        out.append("\n--- Suffix Patterns ('%.suffix') ---")
        # do not traverse char-trie for brevity
        has_suf = bool(self._suffix_root.children or self._suffix_root.subs)
        out.append("(None)" if not has_suf else "(present)")

        out.append("\n--- Regex Fallback ---")
        if not self._regex_bucket:
            out.append("(None)")
        else:
            for pat, (_cre, subs) in sorted(self._regex_bucket.items(), key=lambda x: x[0]):
                names = self._format_sub_list(subs) or "[]"
                out.append(f"- Pattern: '{pat}' -> Subs: {names}")

        out.append("\n--- Match-All ('%') ---")
        out.append("(None)" if not self._match_all else f"Subs: {self._format_sub_list(self._match_all)}")

        return "\n".join(out)

    def __repr__(self) -> str:
        n_regex = len(self._regex_bucket)
        n_tracked = len(self._subscriber_locations)
        n_root_children = len(self._root.children)
        n_root_hash = len(self._root.hash_subs)
        if 0 < n_regex <= 5:
            pat_str = f" patterns={list(self._regex_bucket.keys())}"
        elif n_regex > 5:
            pat_str = f" patterns=[...{n_regex}...]"
        else:
            pat_str = ""
        return (
            f"<PubSubRouter(delimiter='{self.delimiter}', "
            f"trie[root_children={n_root_children}, root_hash_subs={n_root_hash}], "
            f"star={'yes' if (self._star_root.children or self._star_root.star or self._star_root.subs) else 'no'}, "
            f"suffix={'yes' if (self._suffix_root.children or self._suffix_root.subs) else 'no'}, "
            f"regex[count={n_regex}{pat_str}], "
            f"tracked_subscribers={n_tracked})>"
        )
