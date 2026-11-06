"""Dataproc instance selection and CLI parsing."""

import argparse
import ast
import re

from encap_lib.backends import ConfigurationError


def instance_ids(value):
    if type(value) is int:
        if value <= 0:
            raise ConfigurationError("Instance count must be positive")
        return list(range(value))
    if (not isinstance(value, list) or not value
            or any(type(item) is not int or item < 0 for item in value)
            or len(set(value)) != len(value)):
        raise ConfigurationError("Instances must be a positive count or a nonempty list of unique nonnegative integers")
    return list(value)


def parse_instances(text):
    try:
        if re.fullmatch(r"range\(\s*-?\d+(?:\s*,\s*-?\d+){0,2}\s*\)", text):
            value = list(range(*ast.literal_eval("(" + text[6:-1] + ",)")))
        else:
            value = ast.literal_eval(text)
        instance_ids(value)
        return value
    except (ValueError, SyntaxError, TypeError, ConfigurationError) as error:
        raise argparse.ArgumentTypeError(str(error)) from error
