# External module dependencies
from dataclasses import is_dataclass
from inspect import signature
import types

###############################################################################
# Functions
###############################################################################
def decode(T, value):
    def _simple(T, value):
        S = type(value)
        if T == S: return value
        raise TypeError('Expected a type of %s but got %s for value \"%s\"' % (
            T.__name__, S.__name__, str(value)
        ))

    def _list(T, value):
        if not isinstance(value, list):
            raise TypeError('Expected a type of list[T] but got %s for value \"%s\"' % (
                type(value).__name__, str(value)
            ))
        args = T.__args__
        if len(args) != 1:
            raise TypeError('Expected list type to have exactly one parameter')
        return [ decode(args[0], item) for item in value ]

    def _dict(T, value):
        if not isinstance(value, dict):
            raise TypeError('Expected a type of dict[str, T] but got %s for value \"%s\"' % (
                type(value).__name__, str(value)
            ))
        args = T.__args__
        if len(args) != 2:
            raise TypeError('Expected dict type to have exactly two parameters')
        if args[0] != str:
            raise TypeError('Expected key type to be str but got %s' % (
                args[0].__name__
            ))
        for key in value.keys():
            if isinstance(key, str): continue
            raise TypeError('Expected key %s to be of type %s but got %s' % (
                key, str.__name__, type(key).__name__
            ))
        return { k: decode(args[1], v) for k, v in value.items() }

    def _intrinsic(T, value):
        if T in [str, int, float]: return _simple(T, value)
        if not isinstance(T, types.GenericAlias):
            raise TypeError('Non-simple types must be parameterized')
        if T.__origin__ == list: return _list(T, value)
        if T.__origin__ == dict: return _dict(T, value)
        raise TypeError('Unsupported dataspec type %s' % T.__name__)

    if not is_dataclass(T): return _intrinsic(T, value)
    if not isinstance(value, dict):
        raise TypeError('Expected a type of dict but got %s for value \"%s\"' % (
            type(value).__name__, str(value)
        ))
    result = {}
    for l, t in signature(T).parameters.items():
        if l not in value:
            raise TypeError('Expected dict to define field %s' % l)
        result[l] = decode(t.annotation, value[l])
    return T(**result)

def encode(T, value):
    def _simple(T, value):
        S = type(value)
        if T == S: return value
        raise TypeError('Expected a type of %s but got %s for value \"%s\"' % (
            T.__name__, S.__name__, str(value)
        ))

    def _list(T, value):
        if not isinstance(value, list):
            raise TypeError('Expected a type of list[T] but got %s for value \"%s\"' % (
                type(value).__name__, str(value)
            ))
        args = T.__args__
        if len(args) != 1:
            raise TypeError('Expected list type to have exactly one parameter')
        return [ encode(args[0], item) for item in value ]

    def _dict(T, value):
        if not isinstance(value, dict):
            raise TypeErro('Expected a type of dict[str, T] but got %s for value \"%s\"' % (
                type(value).__name__, str(value)
            ))
        args = T.__args__
        if len(args) != 2:
            raise TypeError('Expected dict type to have exactly two parameters')
        if args[0] != str:
            raise TypeError('Expected type to be str but got %s' % (
                args[0].__name__
            ))
        for key in value.keys():
            if isinstance(key, str): continue
            raise TypeError('Expected key %s to be of type %s but got %s' % (
                key, str.__name__, type(key).__name__
            ))
        return { k: encode(args[1], v) for k, v in value.items() }

    def _intrinsic(T, value):
        if T in [str, int, float]: return _simple(T, value)
        if not isinstance(T, types.GenericAlias):
            raise TypeError('Non-simple types must be parameterized')
        if T.__origin__ == list: return _list(T, value)
        if T.__origin__ == dict: return _dict(T, value)
        raise TypeError('Unsupported dataspec type %s' % T.__name__)

    if not is_dataclass(T): return _intrinsic(T, value)
    result = {}
    for l, t in signature(T).parameters.items():
        if l not in value.__dict__:
            raise TypeError('Expected dataclass to define field %s' % l)
        result[l] = encode(t.annotation, value.__dict__[l])
    return result
