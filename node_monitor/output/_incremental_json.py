"""node_monitor.output._incremental_json -- a hand-rolled, character-at-
a-time JSON grammar validator used ONLY as a bounded-memory fallback
inside ``scan_jsonl_artifact`` (node_monitor/output/jsonl.py) for the
rare line that exceeds the fast-path buffering threshold.

Why this exists (review history on the Phase 0 streaming-finalize
fix): the fast path buffers one line at a time in a ``bytearray`` and
calls ``json.loads`` on it once complete -- fast, and exactly matches
``json.loads``'s verdict, but its memory use is proportional to that
one line's length. A first fix attempt capped line length and reported
anything past the cap as malformed/truncated; that silently broke
validation for legitimate large JSON records (Phase 0 places no upper
bound on a single record's size), which review correctly rejected as
a "preserve public behavior" violation. A second attempt removed the
cap entirely, which reintroduced the *original* unbounded-memory
defect for a pathological/corrupted line with no newline (measured:
+372 MB RSS scanning a 128 MiB newline-free file).

Both constraints -- O(bounded) memory AND exact ``json.loads`` parity
for arbitrarily large but well-formed input -- cannot be satisfied by
buffering bytes and later doing one big ``json.loads`` call, no matter
what the buffering strategy is. They require a validator that consumes
the line's bytes incrementally and holds only grammar STATE (which
container we are nested inside, and how deep), never the line's
CONTENT. This module is that validator: memory is a stack of tiny
(kind, substate) frames -- one per currently-open ``{``/``[`` -- plus a
handful of scalar counters. It never buffers a string/number token or
the line itself, so it scales with JSON *nesting depth*, not with
token length or line length.

This is intentionally NOT a general-purpose JSON parser (it discards
values; it only answers "is this valid JSON per the same rules
``json.loads`` uses" for one line). Grammar matched deliberately
identically to Python's stdlib ``json`` module, including its
documented non-standard extensions accepted by ``json.loads``: NaN,
Infinity, -Infinity. Verified against ``json.loads`` with a large
randomized/mutation-based fuzz comparison during development (see the
test suite's ``TestIncrementalJsonValidator`` for the checked-in
subset); used here purely as a memory-bounded fallback for the
uncommon oversized-line case, not as the primary validation path for
ordinary lines (the fast path already agrees with ``json.loads``
exactly, and is far faster for typical Phase 0 record sizes).
"""

_WS = " \t\n\r"

_OBJ = 0
_ARR = 1

# Object substates
_OBJ_EMPTY_OR_KEY = 0     # just saw '{': expect '}' (empty) or a key
_OBJ_KEY_REQUIRED = 1     # just saw ',': expect a key (no '}' allowed)
_OBJ_EXPECT_COLON = 2
_OBJ_EXPECT_VALUE = 3
_OBJ_EXPECT_COMMA_OR_END = 4

# Array substates
_ARR_EMPTY_OR_VALUE = 5   # just saw '[': expect ']' (empty) or a value
_ARR_VALUE_REQUIRED = 6   # just saw ',': expect a value (no ']' allowed)
_ARR_EXPECT_COMMA_OR_END = 7

_NUM_START = 0
_NUM_INT_ZERO = 1
_NUM_INT_DIGITS = 2
_NUM_FRAC_START = 3
_NUM_FRAC_DIGITS = 4
_NUM_EXP_START = 5
_NUM_EXP_SIGN = 6
_NUM_EXP_DIGITS = 7

_DIGITS = "0123456789"


class JsonSyntaxError(Exception):
    """Raised by IncrementalJsonValidator as soon as fed input is
    provably not valid JSON. Analogous to json.JSONDecodeError, but
    this module intentionally does not depend on the json module's
    internals -- it independently re-implements the grammar so it can
    be fed one character/chunk at a time."""


class IncrementalJsonValidator:
    """Feed text via ``feed_str()`` any number of times (across chunk
    boundaries, one character at a time, or all at once -- the result
    is identical either way), then call ``finish()``. Raises
    ``JsonSyntaxError`` as soon as the input consumed so far is
    provably invalid; ``finish()`` raises it too if the input ended
    without completing exactly one valid JSON value.

    Memory: a stack of tiny (kind, substate) frames -- one per
    currently-open ``{``/``[`` -- plus a handful of scalar counters.
    Never a buffer sized to a string/number token's length or to the
    total input consumed so far.
    """

    def __init__(self):
        self._stack = []
        self._mode = "value"      # "value" | "after_value" | "done"
        self._in_string = False
        self._str_escape = False
        self._str_unicode_remaining = 0
        self._is_key = False
        self._lit_target = None
        self._lit_pos = 0
        self._num_state = None
        self._num_is_neg_infinity_candidate = False
        self._pos = 0
        self._saw_value = False

    def feed_str(self, text):
        for ch in text:
            self._feed_one(ch)

    def finish(self):
        if self._in_string:
            raise JsonSyntaxError("unterminated string")
        if self._lit_target is not None:
            raise JsonSyntaxError("unterminated literal")
        if self._num_state is not None:
            self._end_number()
        if self._stack:
            raise JsonSyntaxError("unterminated container")
        if not self._saw_value:
            raise JsonSyntaxError("expecting value")
        if self._mode != "done":
            raise JsonSyntaxError("incomplete document")

    # ------------------------------------------------------------------
    def _feed_one(self, ch):
        self._pos += 1

        if self._in_string:
            self._feed_string_char(ch)
            return
        if self._lit_target is not None:
            self._feed_literal_char(ch)
            return
        if self._num_state is not None:
            if ch in _DIGITS:
                self._feed_number_digit(ch)
                return
            if ch == "." and self._num_state in (_NUM_INT_ZERO, _NUM_INT_DIGITS):
                self._num_state = _NUM_FRAC_START
                return
            if ch in "eE" and self._num_state in (
                    _NUM_INT_ZERO, _NUM_INT_DIGITS, _NUM_FRAC_DIGITS):
                self._num_state = _NUM_EXP_START
                return
            if ch in "+-" and self._num_state == _NUM_EXP_START:
                self._num_state = _NUM_EXP_SIGN
                return
            if (ch == "I" and self._num_state == _NUM_START
                    and self._num_is_neg_infinity_candidate):
                # A lone '-' immediately followed by 'I' is the
                # -Infinity literal (one of json.loads's documented
                # non-standard constants), not a malformed number.
                self._num_state = None
                self._num_is_neg_infinity_candidate = False
                self._lit_target, self._lit_pos = "Infinity", 1
                return
            self._end_number()
            self._feed_one(ch)
            return

        if self._mode == "done":
            if ch in _WS:
                return
            raise JsonSyntaxError("extra data at %d" % self._pos)

        if self._mode == "value":
            self._begin_value(ch)
            return

        if self._mode == "after_value":
            self._feed_after_value(ch)
            return

        raise JsonSyntaxError("internal state error")

    # -- starting a value ------------------------------------------------
    def _begin_value(self, ch):
        if ch in _WS:
            return

        if self._stack:
            kind, substate = self._stack[-1]
            if kind == _OBJ and substate == _OBJ_EMPTY_OR_KEY:
                if ch == "}":
                    self._stack.pop()
                    self._end_value()
                    return
                if ch != '"':
                    raise JsonSyntaxError(
                        "expecting property name enclosed in double quotes "
                        "at %d" % self._pos)
            elif kind == _OBJ and substate == _OBJ_KEY_REQUIRED:
                if ch != '"':
                    raise JsonSyntaxError(
                        "expecting property name enclosed in double quotes "
                        "at %d" % self._pos)
            elif kind == _ARR and substate == _ARR_EMPTY_OR_VALUE:
                if ch == "]":
                    self._stack.pop()
                    self._end_value()
                    return

        if ch == "{":
            self._stack.append([_OBJ, _OBJ_EMPTY_OR_KEY])
            return
        if ch == "[":
            self._stack.append([_ARR, _ARR_EMPTY_OR_VALUE])
            return
        if ch == '"':
            self._in_string = True
            self._str_escape = False
            self._str_unicode_remaining = 0
            self._is_key = bool(
                self._stack and self._stack[-1][0] == _OBJ
                and self._stack[-1][1] in (_OBJ_EMPTY_OR_KEY, _OBJ_KEY_REQUIRED))
            return
        if ch == "-":
            self._num_state = _NUM_START
            self._num_is_neg_infinity_candidate = True
            return
        if ch in _DIGITS:
            self._num_state = _NUM_INT_ZERO if ch == "0" else _NUM_INT_DIGITS
            return
        if ch == "t":
            self._lit_target, self._lit_pos = "true", 1
            return
        if ch == "f":
            self._lit_target, self._lit_pos = "false", 1
            return
        if ch == "n":
            self._lit_target, self._lit_pos = "null", 1
            return
        if ch == "N":
            self._lit_target, self._lit_pos = "NaN", 1
            return
        if ch == "I":
            self._lit_target, self._lit_pos = "Infinity", 1
            return
        raise JsonSyntaxError("expecting value at %d: %r" % (self._pos, ch))

    # -- strings -----------------------------------------------------
    def _feed_string_char(self, ch):
        if self._str_unicode_remaining > 0:
            if ch not in "0123456789abcdefABCDEF":
                raise JsonSyntaxError("invalid \\u escape at %d" % self._pos)
            self._str_unicode_remaining -= 1
            return
        if self._str_escape:
            if ch in '"\\/bfnrt':
                self._str_escape = False
                return
            if ch == "u":
                self._str_unicode_remaining = 4
                self._str_escape = False
                return
            raise JsonSyntaxError("invalid escape at %d" % self._pos)
        if ch == "\\":
            self._str_escape = True
            return
        if ch == '"':
            self._in_string = False
            if self._is_key:
                self._is_key = False
                self._stack[-1][1] = _OBJ_EXPECT_COLON
                self._mode = "after_value"
                return
            self._end_value()
            return
        if ord(ch) < 0x20:
            raise JsonSyntaxError("invalid control character at %d" % self._pos)
        # Any other character (including astral code points and lone
        # surrogates, which json.loads also accepts unpaired) is fine
        # -- its content is never retained beyond this single character.

    # -- literals (true/false/null/NaN/Infinity) ----------------------
    def _feed_literal_char(self, ch):
        target = self._lit_target
        if target[self._lit_pos] != ch:
            raise JsonSyntaxError("invalid literal at %d" % self._pos)
        self._lit_pos += 1
        if self._lit_pos == len(target):
            self._lit_target = None
            self._lit_pos = 0
            self._end_value()

    # -- numbers -------------------------------------------------------
    def _feed_number_digit(self, ch):
        s = self._num_state
        self._num_is_neg_infinity_candidate = False
        if s == _NUM_START:
            self._num_state = _NUM_INT_ZERO if ch == "0" else _NUM_INT_DIGITS
        elif s == _NUM_INT_ZERO:
            raise JsonSyntaxError("invalid number at %d" % self._pos)
        elif s == _NUM_INT_DIGITS:
            pass
        elif s == _NUM_FRAC_START:
            self._num_state = _NUM_FRAC_DIGITS
        elif s == _NUM_FRAC_DIGITS:
            pass
        elif s == _NUM_EXP_START:
            self._num_state = _NUM_EXP_DIGITS
        elif s == _NUM_EXP_SIGN:
            self._num_state = _NUM_EXP_DIGITS
        elif s == _NUM_EXP_DIGITS:
            pass
        else:
            raise JsonSyntaxError("bad number state")

    def _end_number(self):
        s = self._num_state
        self._num_state = None
        if s in (_NUM_INT_ZERO, _NUM_INT_DIGITS, _NUM_FRAC_DIGITS, _NUM_EXP_DIGITS):
            self._end_value()
            return
        raise JsonSyntaxError("incomplete number")

    # -- shared post-value transition --------------------------------
    def _end_value(self):
        self._saw_value = True
        if not self._stack:
            self._mode = "done"
            return
        kind, _substate = self._stack[-1]
        if kind == _OBJ:
            self._stack[-1][1] = _OBJ_EXPECT_COMMA_OR_END
        else:
            self._stack[-1][1] = _ARR_EXPECT_COMMA_OR_END
        self._mode = "after_value"

    def _feed_after_value(self, ch):
        if ch in _WS:
            return
        if not self._stack:
            raise JsonSyntaxError("extra data at %d" % self._pos)
        kind, substate = self._stack[-1]
        if kind == _OBJ:
            if substate == _OBJ_EXPECT_COLON:
                if ch != ":":
                    raise JsonSyntaxError("expecting ':' delimiter at %d" % self._pos)
                self._stack[-1][1] = _OBJ_EXPECT_VALUE
                self._mode = "value"
                return
            if substate == _OBJ_EXPECT_COMMA_OR_END:
                if ch == "}":
                    self._stack.pop()
                    self._end_value()
                    return
                if ch == ",":
                    self._stack[-1][1] = _OBJ_KEY_REQUIRED
                    self._mode = "value"
                    return
                raise JsonSyntaxError("expecting ',' delimiter at %d" % self._pos)
            raise JsonSyntaxError("bad object state")
        else:
            if substate == _ARR_EXPECT_COMMA_OR_END:
                if ch == "]":
                    self._stack.pop()
                    self._end_value()
                    return
                if ch == ",":
                    self._stack[-1][1] = _ARR_VALUE_REQUIRED
                    self._mode = "value"
                    return
                raise JsonSyntaxError("expecting ',' delimiter at %d" % self._pos)
            raise JsonSyntaxError("bad array state")
