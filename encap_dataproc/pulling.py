"""Translate shared rsync pull exclusions without changing their pattern language."""

import re

from encap_lib.backends import ConfigurationError, validate_pull_exclusions

INTERNAL_EXCLUDE = r"(^|.*/)\.encap-run(/.*|$)"


def _translate(pattern: str) -> str:
    directory = pattern.endswith("/")
    anchored = pattern.startswith("/")
    body = pattern[1:] if anchored else pattern
    body = body[:-1] if directory else body
    if not body or any(part in ("", ".", "..") for part in body.split("/")):
        raise ConfigurationError(f"Invalid rsync_exclude_pull path: {pattern!r}")
    if "***" in body:
        raise ConfigurationError(f"Unsupported rsync_exclude_pull pattern: {pattern!r}")
    wildcard = any(char in body for char in "*?[")
    if "\\" in body and not wildcard:
        raise ConfigurationError(f"Literal backslash paths are unsupported in rsync_exclude_pull: {pattern!r}")
    prefix = "^" if anchored else r"(?:^|.*/)"
    parts = [prefix]
    index = 0
    while index < len(body):
        char = body[index]
        if char == "\\":
            index += 1
            if index == len(body):
                raise ConfigurationError(f"Unfinished escape in rsync_exclude_pull: {pattern!r}")
            parts.append(re.escape(body[index]))
        elif char == "*":
            if body[index:index + 2] == "**":
                index += 1
                if index == 1 and body[index + 1:index + 2] == "/":
                    parts.append(r"(?:.*/)?")
                    index += 1
                else:
                    parts.append(".*")
            else:
                parts.append("[^/]*")
        elif char == "?":
            parts.append("[^/]")
        elif char == "[":
            index += 1
            negate = index < len(body) and body[index] in "!^"
            if negate:
                index += 1
            characters = []
            if index < len(body) and body[index] == "]":
                characters.append(r"\]")
                index += 1
            while index < len(body) and body[index] != "]":
                value = body[index]
                if body[index:index + 2] in ("[:", "[.", "[="):
                    raise ConfigurationError(f"Unsupported rsync_exclude_pull character class: {pattern!r}")
                if value == "/":
                    raise ConfigurationError(f"Slash in rsync_exclude_pull character class: {pattern!r}")
                if value == "\\":
                    index += 1
                    if index == len(body):
                        break
                    characters.append(re.escape(body[index]))
                elif value == "-":
                    characters.append("-")
                else:
                    characters.append(re.escape(value))
                index += 1
            if index == len(body) or not characters:
                raise ConfigurationError(f"Unfinished character class in rsync_exclude_pull: {pattern!r}")
            parts.append("[" + ("^/" if negate else "") + "".join(characters) + "]")
        else:
            parts.append(re.escape(char))
        index += 1
    parts.append(r"/.*$" if directory else r"(?:/.*)?$")
    # gcloud parses --exclude as a comma-separated list, even without a shell.
    return "".join(parts).replace(",", r"\x2c")


class PullExclusions:
    def __init__(self, values):
        self.patterns = validate_pull_exclusions(values)
        try:
            self.rules = [re.compile(_translate(pattern)) for pattern in self.patterns]
        except re.error as error:
            raise ConfigurationError(f"Invalid rsync_exclude_pull pattern: {error}") from error
        self.regex = (
            "(?:" + "|".join([INTERNAL_EXCLUDE, *(rule.pattern for rule in self.rules)]) + ")"
            if self.rules else INTERNAL_EXCLUDE
        )
        self.compiled = re.compile(self.regex)

    def matches(self, path: str) -> bool:
        return self.compiled.match(path) is not None
