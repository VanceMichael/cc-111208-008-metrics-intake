"""OpenMetrics 1.0 subset parser and the original scalar inspector.

The inspector is kept for backwards compatibility.  The scrape pipeline uses
``parse_exposition``, which validates the complete supported subset before a
batch is offered to the store.
"""

from dataclasses import dataclass
from decimal import Decimal, InvalidOperation
import json
import re


class ParseError(ValueError):
    """The payload is not valid OpenMetrics for the supported subset."""


class DuplicateLabelError(ParseError):
    pass


class DuplicateSampleError(ParseError):
    pass


class HistogramError(ParseError):
    pass


class SeriesBudgetExceeded(ParseError):
    def __init__(self, count, budget):
        super().__init__('batch has %d series, budget is %d' % (count, budget))
        self.count = count
        self.budget = budget


_METRIC_NAME = re.compile(r'[a-zA-Z_:][a-zA-Z0-9_:]*')
_LABEL_NAME = re.compile(r'[a-zA-Z_][a-zA-Z0-9_]*')
_FINITE = re.compile(
    r'[+-]?(?:(?:[0-9]+(?:\.[0-9]*)?)|(?:\.[0-9]+))(?:[eE][+-]?[0-9]+)?')
_TIMESTAMP = re.compile(r'[+-]?(?:[0-9]+(?:\.[0-9]*)?|\.[0-9]+)')
_DIRECTIVE = re.compile(r'# (HELP|TYPE|UNIT) ([a-zA-Z_:][a-zA-Z0-9_:]*)(?: (.*))?$')

SUPPORTED_TYPES = ('gauge', 'counter', 'histogram')
_HISTOGRAM_SUFFIXES = (
    ('_bucket', 'histogram_bucket'),
    ('_sum', 'histogram_sum'),
    ('_count', 'histogram_count'),
)


@dataclass(frozen=True)
class Value:
    text: str
    kind: str                 # finite / nan / posinf / neginf
    number: Decimal = None    # only for kind == finite

    def signature(self):
        # Duplicate samples must be *exactly* identical: the textual value is
        # compared, so 1 and 1.0 conflict rather than collapsing silently.
        return (self.kind, self.text)


@dataclass(frozen=True)
class Exemplar:
    labels: dict
    value: Value
    timestamp_text: str = None

    def as_json(self):
        return {
            'labels': dict(sorted(self.labels.items())),
            'value': self.value.text,
            'value_kind': self.value.kind,
            'timestamp': self.timestamp_text,
        }


@dataclass
class Sample:
    metric_name: str
    labels: dict
    value: Value
    timestamp_text: str = None
    timestamp_ns: int = None
    exemplar: Exemplar = None
    kind: str = 'untyped'
    family_name: str = ''
    unit: str = None
    help: str = None
    occurrences: int = 1
    line_number: int = 0

    @property
    def labels_json(self):
        return canonical_labels(self.labels)

    @property
    def series_key(self):
        return series_identity(self.metric_name, self.labels)

    def duplicate_signature(self):
        exemplar = None
        if self.exemplar is not None:
            exemplar = json.dumps(self.exemplar.as_json(), sort_keys=True,
                                  ensure_ascii=False, separators=(',', ':'))
        return (self.value.signature(), self.timestamp_text, exemplar)


@dataclass
class Family:
    name: str
    kind: str
    help: str = None
    unit: str = None


@dataclass
class Exposition:
    families: dict
    samples: list
    sample_lines: int

    @property
    def series_count(self):
        return len(self.samples)


def canonical_labels(labels):
    return json.dumps(labels, sort_keys=True, ensure_ascii=False, separators=(',', ':'))


def series_identity(metric_name, labels):
    """The identity does not depend on label order or the endpoint.

    The endpoint is applied by the store because the same device series on two
    endpoints are distinct identities.
    """
    return metric_name + '\x1f' + canonical_labels(labels)


def _parse_value(token, line_number):
    if token in ('NaN', 'nan'):
        return Value(token, 'nan')
    if token in ('+Inf', 'Inf', 'inf', '+inf'):
        return Value(token, 'posinf')
    if token in ('-Inf', '-inf'):
        return Value(token, 'neginf')
    if not _FINITE.fullmatch(token):
        raise ParseError('line %d: invalid sample value %r' % (line_number, token))
    try:
        number = Decimal(token)
    except InvalidOperation:
        raise ParseError('line %d: invalid sample value %r' % (line_number, token))
    # A literal may be mathematically finite but outside binary float range.
    # The exact Decimal stays available; value_real is left unset by the store.
    return Value(token, 'finite', number)


def _parse_timestamp(token, line_number, what='timestamp'):
    """Return the timestamp exactly as written plus nanoseconds since epoch.

    OpenMetrics timestamps are seconds; fractional seconds are preserved in
    the original text and converted to integer nanoseconds without float
    round-tripping.  Negative device timestamps are kept, not clamped.
    """
    if not _TIMESTAMP.fullmatch(token):
        raise ParseError('line %d: invalid %s %r' % (line_number, what, token))
    negative = token.startswith('-')
    body = token.lstrip('+-')
    whole, _, fraction = body.partition('.')
    fraction = (fraction + '000000000')[:9]
    nanoseconds = int(whole or '0') * 1000000000 + int(fraction)
    if negative:
        nanoseconds = -nanoseconds
    return token, nanoseconds


def _skip_space(text, position):
    while position < len(text) and text[position] in ' \t':
        position += 1
    return position


def _read_token(text, position, line_number, what):
    start = position
    while position < len(text) and text[position] not in ' \t':
        position += 1
    if position == start:
        raise ParseError('line %d: missing %s' % (line_number, what))
    return text[start:position], position


def _parse_labels(text, position, line_number):
    """Parse ``{...}`` starting at position and return (labels, next position)."""
    if text[position] != '{':
        raise ParseError('line %d: expected label set' % line_number)
    position += 1
    labels = {}
    position = _skip_space(text, position)
    if position < len(text) and text[position] == '}':
        return labels, position + 1
    while True:
        position = _skip_space(text, position)
        match = _LABEL_NAME.match(text, position)
        if not match:
            raise ParseError('line %d: invalid label name' % line_number)
        name = match.group(0)
        position = _skip_space(text, match.end())
        if position >= len(text) or text[position] != '=':
            raise ParseError('line %d: expected = after label %s' % (line_number, name))
        position = _skip_space(text, position + 1)
        if position >= len(text) or text[position] != '"':
            raise ParseError('line %d: label %s value must be quoted' % (line_number, name))
        position += 1
        chars = []
        while True:
            if position >= len(text):
                raise ParseError('line %d: unterminated label value' % line_number)
            char = text[position]
            if char == '"':
                position += 1
                break
            if char == '\\':
                position += 1
                if position >= len(text):
                    raise ParseError('line %d: unterminated escape' % line_number)
                escaped = text[position]
                if escaped == 'n':
                    chars.append('\n')
                elif escaped == '"':
                    chars.append('"')
                elif escaped == '\\':
                    chars.append('\\')
                else:
                    raise ParseError('line %d: unsupported label escape \\%s'
                                     % (line_number, escaped))
                position += 1
                continue
            if char in '\r\n':
                raise ParseError('line %d: raw newline in label value' % line_number)
            chars.append(char)
            position += 1
        if name in labels:
            raise DuplicateLabelError('line %d: duplicate label %s' % (line_number, name))
        labels[name] = ''.join(chars)
        position = _skip_space(text, position)
        if position >= len(text):
            raise ParseError('line %d: unterminated label set' % line_number)
        if text[position] == ',':
            position += 1
            if _skip_space(text, position) >= len(text) or text[_skip_space(text, position)] == '}':
                raise ParseError('line %d: trailing comma in label set' % line_number)
            continue
        if text[position] == '}':
            return labels, position + 1
        raise ParseError('line %d: expected , or } in label set' % line_number)


def _parse_exemplar(text, position, line_number):
    if text[position] != '#':
        raise ParseError('line %d: expected exemplar marker' % line_number)
    position = _skip_space(text, position + 1)
    if position >= len(text) or text[position] != '{':
        raise ParseError('line %d: exemplar must start with a label set' % line_number)
    labels, position = _parse_labels(text, position, line_number)
    position = _skip_space(text, position)
    value_token, position = _read_token(text, position, line_number, 'exemplar value')
    value = _parse_value(value_token, line_number)
    position = _skip_space(text, position)
    timestamp = None
    if position < len(text):
        token, position = _read_token(text, position, line_number, 'exemplar timestamp')
        timestamp, _ = _parse_timestamp(token, line_number, 'exemplar timestamp')
        position = _skip_space(text, position)
        if position < len(text):
            raise ParseError('line %d: trailing tokens after exemplar' % line_number)
    return Exemplar(labels, value, timestamp), position


def _parse_sample(line, line_number):
    match = _METRIC_NAME.match(line)
    if not match:
        raise ParseError('line %d: invalid metric name' % line_number)
    metric_name = match.group(0)
    position = match.end()
    labels = {}
    if position < len(line) and line[position] == '{':
        labels, position = _parse_labels(line, position, line_number)
    if position < len(line) and line[position] not in ' \t':
        raise ParseError('line %d: expected whitespace after series' % line_number)
    position = _skip_space(line, position)
    if position >= len(line):
        raise ParseError('line %d: sample has no value' % line_number)
    value_token, position = _read_token(line, position, line_number, 'sample value')
    value = _parse_value(value_token, line_number)
    position = _skip_space(line, position)
    timestamp = None
    timestamp_ns = None
    exemplar = None
    if position < len(line):
        if line[position] == '#':
            exemplar, position = _parse_exemplar(line, position, line_number)
        else:
            timestamp_token, position = _read_token(line, position, line_number, 'timestamp')
            timestamp, timestamp_ns = _parse_timestamp(timestamp_token, line_number)
            position = _skip_space(line, position)
            if position < len(line):
                if line[position] == '#':
                    exemplar, position = _parse_exemplar(line, position, line_number)
                else:
                    raise ParseError('line %d: trailing tokens after timestamp' % line_number)
    return Sample(metric_name, labels, value, timestamp, timestamp_ns, exemplar,
                  line_number=line_number)


def _parse_directive(line, line_number, families):
    match = _DIRECTIVE.match(line)
    if not match:
        # A line that looks like a directive but is malformed must not be
        # silently treated as a comment.
        if line.startswith('# HELP') or line.startswith('# TYPE') or line.startswith('# UNIT'):
            raise ParseError('line %d: malformed metadata directive' % line_number)
        return
    directive, name, argument = match.groups()
    argument = '' if argument is None else argument
    if directive == 'TYPE':
        if not argument:
            raise ParseError('line %d: TYPE has no metric type' % line_number)
        if argument not in SUPPORTED_TYPES:
            raise ParseError('line %d: unsupported metric type %r (native histograms are not supported)'
                             % (line_number, argument))
        family = families.get(name)
        if family is not None and family.kind is not None and family.kind != argument:
            raise ParseError('line %d: conflicting TYPE for %s' % (line_number, name))
        families[name] = Family(name, argument,
                                family.help if family else None,
                                family.unit if family else None)
    elif directive == 'HELP':
        if not argument:
            raise ParseError('line %d: HELP has no text' % line_number)
        old = families.get(name)
        families[name] = Family(name, old.kind if old else None, argument,
                                old.unit if old else None)
    elif directive == 'UNIT':
        if not argument or len(argument.split()) != 1:
            raise ParseError('line %d: invalid UNIT' % line_number)
        old = families.get(name)
        families[name] = Family(name, old.kind if old else None, old.help if old else None,
                                argument)


def _split_lines(payload):
    try:
        text = payload.decode('utf-8')
    except UnicodeDecodeError as error:
        raise ParseError('payload is not valid UTF-8: %s' % error)
    if text.startswith('﻿'):
        text = text[1:]
    lines = text.split('\n')
    if lines and lines[-1] == '':
        lines.pop()
    return [line[:-1] if line.endswith('\r') else line for line in lines]


def _check_eof(lines):
    content = [index for index, line in enumerate(lines) if line.strip()]
    if not content:
        raise ParseError('missing required # EOF')
    last = content[-1]
    if lines[last].strip() != '# EOF':
        raise ParseError('missing required # EOF at end of payload')
    for index in content:
        if index != last and lines[index].strip() == '# EOF':
            raise ParseError('line %d: # EOF before end of payload' % (index + 1))
    return last


def _classify_sample(sample, families):
    family = families.get(sample.metric_name)
    if family is not None:
        if family.kind == 'histogram':
            raise HistogramError('line %d: histogram family %s must use _bucket/_sum/_count samples'
                                 % (sample.line_number, sample.metric_name))
        sample.kind = family.kind or 'untyped'
        sample.family_name = family.name
        sample.help = family.help
        sample.unit = family.unit
        return
    for suffix, kind in _HISTOGRAM_SUFFIXES:
        if sample.metric_name.endswith(suffix):
            base = sample.metric_name[:-len(suffix)]
            base_family = families.get(base)
            if base_family is not None and base_family.kind == 'histogram':
                sample.kind = kind
                sample.family_name = base
                sample.help = base_family.help
                sample.unit = base_family.unit
                return
    sample.kind = 'untyped'
    sample.family_name = sample.metric_name
    direct = families.get(sample.metric_name)
    if direct is not None:
        sample.help = direct.help
        sample.unit = direct.unit


def _deduplicate(raw_samples):
    collapsed = []
    first_by_key = {}
    for sample in raw_samples:
        key = sample.series_key
        first = first_by_key.get(key)
        if first is None:
            first_by_key[key] = sample
            collapsed.append(sample)
            continue
        if first.duplicate_signature() != sample.duplicate_signature():
            raise DuplicateSampleError(
                'line %d: sample for series %s conflicts with line %d; '
                'duplicate samples must be exactly identical'
                % (sample.line_number, sample.metric_name, first.line_number))
        first.occurrences += 1
    return collapsed


def _label_group_key(labels):
    """Canonical labels with the histogram ``le`` label removed."""
    return canonical_labels({name: value for name, value in labels.items()
                             if name != 'le'})


def _finite_non_negative(value, line_number, what):
    if value.kind != 'finite':
        raise HistogramError('line %d: %s must be a finite number' % (line_number, what))
    if value.number < 0:
        raise HistogramError('line %d: %s must be non-negative' % (line_number, what))
    return value.number


def _bucket_boundary(value, line_number):
    if value.kind == 'posinf':
        return None                 # sorts after every finite boundary
    if value.kind != 'finite':
        raise HistogramError('line %d: histogram le must be finite or +Inf' % line_number)
    return value.number


def _validate_histograms(samples):
    """Validate every histogram family's components.

    OpenMetrics allows ``_sum``/``_count`` without any bucket when there are no
    observations.  When buckets exist, their ``le`` values are sorted
    numerically, cumulative counts must not decrease, ``+Inf`` must be present,
    and the ``+Inf`` bucket must equal ``_count``.
    """
    histograms = {}
    for sample in samples:
        if sample.kind.startswith('histogram_'):
            histograms.setdefault(sample.family_name, []).append(sample)

    for family_name, components in histograms.items():
        groups = {}
        for sample in components:
            group_key = _label_group_key(sample.labels)
            group = groups.setdefault(group_key,
                                      {'buckets': [], 'count': None, 'sum': None})
            if sample.kind == 'histogram_bucket':
                if 'le' not in sample.labels:
                    raise HistogramError('line %d: histogram bucket is missing le'
                                         % sample.line_number)
                boundary = _bucket_boundary(
                    _parse_value(sample.labels['le'], sample.line_number),
                    sample.line_number)
                group['buckets'].append((boundary, sample))
            else:
                if 'le' in sample.labels:
                    raise HistogramError('line %d: %s sample must not carry le'
                                         % (sample.line_number, sample.kind))
                if sample.kind == 'histogram_count':
                    group['count'] = sample
                else:
                    group['sum'] = sample

        for group_key, group in groups.items():
            if not group['buckets']:
                # No observations: count (and sum) may stand alone.
                if group['count'] is None:
                    raise HistogramError('histogram %s has no buckets and no _count '
                                         'for label set %s' % (family_name, group_key))
                _finite_non_negative(group['count'].value, group['count'].line_number,
                                     'histogram count')
                continue
            boundaries = []
            seen = {}
            for boundary, sample in group['buckets']:
                marker = ('+Inf',) if boundary is None else ('finite', boundary)
                if marker in seen:
                    raise HistogramError('duplicate histogram boundary %s for label set %s'
                                         % (sample.labels.get('le'), group_key))
                seen[marker] = sample
                value_number = _finite_non_negative(sample.value, sample.line_number,
                                                    'histogram bucket count')
                sort_key = (1, Decimal(0)) if boundary is None else (0, boundary)
                boundaries.append((sort_key, boundary, value_number, sample))
            boundaries.sort(key=lambda entry: entry[0])
            inf_entries = [entry for entry in boundaries if entry[1] is None]
            if not inf_entries:
                raise HistogramError('histogram %s is missing the +Inf bucket for label set %s'
                                     % (family_name, group_key))
            previous = None
            for _, _, number, sample in boundaries:
                if previous is not None and number < previous:
                    raise HistogramError('line %d: cumulative histogram buckets must not decrease'
                                         % sample.line_number)
                previous = number
            if group['count'] is None:
                raise HistogramError('histogram %s is missing _count for label set %s'
                                     % (family_name, group_key))
            count = _finite_non_negative(group['count'].value, group['count'].line_number,
                                         'histogram count')
            inf_bucket = inf_entries[0]
            if inf_bucket[2] != count:
                raise HistogramError('line %d: +Inf bucket count %s does not equal count %s'
                                     % (inf_bucket[3].line_number,
                                        inf_bucket[3].value.text, group['count'].value.text))


def parse_exposition(payload, series_budget=None):
    """Parse and validate a complete exposition payload.

    ``payload`` includes the terminating ``# EOF``.  Raises ParseError
    subclasses for any invalid payload; no partial result is returned.
    """
    lines = _split_lines(payload)
    eof_index = _check_eof(lines)
    families = {}
    raw_samples = []
    for index in range(eof_index):
        line = lines[index]
        if not line:
            continue
        if line.lstrip().startswith('#'):
            _parse_directive(line.lstrip(), index + 1, families)
            continue
        if line != line.lstrip():
            raise ParseError('line %d: unexpected indentation' % (index + 1))
        raw_samples.append(_parse_sample(line, index + 1))

    for sample in raw_samples:
        _classify_sample(sample, families)

    samples = _deduplicate(raw_samples)
    if series_budget is not None and len(samples) > series_budget:
        raise SeriesBudgetExceeded(len(samples), series_budget)
    _validate_histograms(samples)
    return Exposition(families, samples, len(raw_samples))


# The original inspector intentionally stays narrow.  It is not the scrape
# parser and continues to reject labelled series rather than mis-splitting them.
def inspect(payload):
    text = payload.decode('utf-8')
    families = {}
    samples = []
    for line in text.splitlines():
        if line.startswith('# TYPE '):
            fields = line.split()
            if len(fields) != 4:
                raise ValueError('Malformed TYPE directive')
            families[fields[2]] = fields[3]
        elif line and not line.startswith('#'):
            match = re.fullmatch(r'([a-zA-Z_:][a-zA-Z_0-9:]*) ([+-]?(?:[0-9.]+|Inf)|NaN)', line)
            if not match:
                raise ValueError('Inspector accepts only unlabelled scalar samples')
            samples.append({'name': match[1], 'value': match[2]})
    return {'types': families, 'samples': samples, 'eof': text.rstrip().endswith('# EOF')}
