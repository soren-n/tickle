# External module dependencies
from pathlib import Path

###############################################################################
# Depfiles
###############################################################################
def parse(text : str) -> list[str]:
    """The prerequisites in a depfile in the make syntax compilers write
    (gcc -MD, clang -MD): rules of targets, a colon and prerequisites, with
    backslash-newline continuing a rule, a backslash escaping a space, and
    $$ standing for $. Every rule's prerequisites count, in order, once."""
    text = text.replace('\\\r\n', ' ').replace('\\\n', ' ')
    result : list[str] = []
    seen : set[str] = set()
    for line in text.splitlines():
        words = _words(line)
        # The targets end at the first word ending with an unescaped colon
        for index, (_, colon) in enumerate(words):
            if not colon: continue
            for dep, _ in words[index + 1:]:
                if dep == '' or dep in seen: continue
                seen.add(dep)
                result.append(dep)
            break
    return result

def _words(line : str) -> list[tuple[str, bool]]:
    # Words split on unescaped whitespace; each tells whether it ended with
    # the colon of a rule, which is a colon followed by whitespace or the
    # end of the line (so C:/x on Windows stays one word)
    words : list[tuple[str, bool]] = []
    word = ''
    index = 0
    while index < len(line):
        char = line[index]
        if char == '\\' and index + 1 < len(line) and line[index + 1] in ' #\\':
            word += line[index + 1]
            index += 2
            continue
        if char == '$' and index + 1 < len(line) and line[index + 1] == '$':
            word += '$'
            index += 2
            continue
        if char == '#' and word == '':
            break
        if char in ' \t':
            if word != '': words.append((word, False))
            word = ''
            index += 1
            continue
        if char == ':' and (index + 1 == len(line) or line[index + 1] in ' \t'):
            words.append((word, True))
            word = ''
            index += 1
            continue
        word += char
        index += 1
    if word != '': words.append((word, False))
    return words

def load(depfile_path : Path) -> list[str] | None:
    """The prerequisites of a depfile, or None if it cannot be read."""
    try: text = depfile_path.read_text(encoding = 'utf-8', errors = 'replace')
    except OSError: return None
    return parse(text)
