import re


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
