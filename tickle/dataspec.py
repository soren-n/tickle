# External module dependencies
from dataclasses import fields, is_dataclass, MISSING
from typing import cast, get_origin, get_args, Any, Union
from collections.abc import Callable

###############################################################################
# Type
###############################################################################
DataspecValue = Union[
    bool, int, float, str,
    'Dataspec'
]
Dataspec = Union[
    list['DataspecValue'],
    dict[str, 'DataspecValue']
]

###############################################################################
# Functions
###############################################################################
class _Field:
    """A field of a dataclass: its name, its type, how to make its default
    when a document leaves it out (None if it is required), and whether to
    leave it out of an encoded document while it holds its default."""
    __slots__ = ('name', 'type', 'default', 'omit_default')

    def __init__(self,
        name : str, type : Any,
        default : Callable[[], Any] | None, omit_default : bool
        ):
        self.name = name
        self.type = type
        self.default = default
        self.omit_default = omit_default

# The fields of each dataclass; looking them up for every value took half
# the time of decoding a large agenda
_FIELDS : dict[type, list[_Field]] = {}

def _fields(T : type) -> list[_Field]:
    if T not in _FIELDS:
        result : list[_Field] = []
        for item in fields(cast(Any, T)):
            default : Callable[[], Any] | None = None
            if item.default is not MISSING:
                default = (lambda value = item.default: value)
            elif item.default_factory is not MISSING:
                default = item.default_factory
            result.append(_Field(
                item.name, item.type, default,
                bool(item.metadata.get('omit_default', False))
            ))
        _FIELDS[T] = result
    return _FIELDS[T]

_SIMPLE = [bool, str, int, float]

class DecodeError(TypeError):
    """A value that does not fit its type, with where it was found."""
    def __init__(self, message : str, where : list[str]):
        super().__init__(message)
        self.message = message
        self.where = where

    def __str__(self) -> str:
        if len(self.where) == 0: return self.message
        return '%s: %s' % (''.join(self.where).lstrip('.'), self.message)

def _at(where : str, decode : Callable[[], Any]) -> Any:
    try: return decode()
    except DecodeError as error:
        error.where.insert(0, where)
        raise
    except TypeError as error:
        raise DecodeError(str(error), [where]) from error

def decode(T : type, value : Any) -> Any:
    def _value(T : type, value : Any) -> Any:
        if T in _SIMPLE: return _simple(T, value)
        return _document(T, value)

    def _composite(T : type, value : Any) -> Any:
        origin = get_origin(T)
        if origin != None:
            if origin == list: return _list(T, value)
            if origin == dict: return _dict(T, value)
        raise TypeError('Unsupported dataspec type %s' % T.__name__)

    def _document(T : type, value : Any) -> Any:
        if not is_dataclass(T): return _composite(T, value)
        if not isinstance(value, dict):
            raise TypeError(
                'Expected a type of dict but got %s for value \"%s\"' % (
                type(value).__name__, str(value)
            ))
        result = {}
        for item in _fields(T):
            if item.name not in value:
                if item.default is None:
                    raise TypeError('Expected dict to define field %s' % item.name)
                result[item.name] = item.default()
                continue
            result[item.name] = _at('.%s' % item.name, (
                lambda item = item: _value(item.type, value[item.name])
            ))
        return T(**result)

    def _simple(T : type, value : Any) -> Any:
            S = type(value)
            if T == S: return value
            # YAML writes a whole number of seconds without a fraction
            if T == float and S == int: return float(value)
            raise TypeError(
                'Expected a type of %s but got %s for value \"%s\"' % (
                T.__name__, S.__name__, str(value)
            ))

    def _list(T : type, value : Any) -> list[Any]:
        if not isinstance(value, list):
            raise TypeError(
                'Expected a type of List[T] but got %s for value \"%s\"' % (
                type(value).__name__, str(value)
            ))
        value = cast(list[Any], value)
        args = get_args(T)
        if len(args) != 1:
            raise TypeError(
                'Expected list type to have exactly one parameter'
            )
        return [
            _at('[%d]' % index, (lambda item = item: _value(args[0], item)))
            for index, item in enumerate(value)
        ]

    def _dict(T : type, value : Any) -> dict[Any, Any]:
        if not isinstance(value, dict):
            raise TypeError(
                'Expected a type of dict but got %s' % (
                type(value).__name__
            ))
        value = cast(dict[Any, Any], value)
        args = get_args(T)
        if len(args) != 2:
            raise TypeError(
                'Expected dict type to have exactly two parameters'
            )
        if args[0] != str:
            raise TypeError(
                'Expected key type to be str but got %s' % (
                args[0].__name__
            ))
        for key in value.keys():
            if isinstance(key, str): continue
            raise TypeError(
                'Expected key %s to be of type %s but got %s' % (
                key, str.__name__, type(key).__name__
            ))
        return {
            k: _at('.%s' % k, (lambda v = v: _value(args[1], v)))
            for k, v in value.items()
        }

    return _document(T, value)

def encode(T : type, value : Any) -> Dataspec:
    def _value(T : type, value : Any) -> Any:
        if T in _SIMPLE: return _simple(T, value)
        return _document(T, value)

    def _composite(T : type, value : Any) -> Any:
        origin = get_origin(T)
        if origin != None:
            if origin == list: return _list(T, value)
            if origin == dict: return _dict(T, value)
        raise TypeError('Unsupported dataspec type %s' % T.__name__)

    def _document(T : type, value : Any) -> Any:
        if not is_dataclass(T): return _composite(T, value)
        result : dict[str, Any] = {}
        for item in _fields(T):
            if item.name not in value.__dict__:
                raise TypeError('Expected dataclass to define field %s' % item.name)
            field_value = value.__dict__[item.name]
            if (item.omit_default and item.default is not None
                and field_value == item.default()):
                continue
            result[item.name] = _value(item.type, field_value)
        return result

    def _simple(T : type, value : Any) -> Any:
        S = type(value)
        if T == S: return value
        if T == float and S == int: return float(value)
        raise TypeError(
            'Expected a type of %s but got %s for value \"%s\"' % (
            T.__name__, S.__name__, str(value)
        ))

    def _list(T : type, value : Any) -> Any:
        if not isinstance(value, list):
            raise TypeError(
                'Expected a type of List[T] but got %s for value \"%s\"' % (
                type(value).__name__, str(value)
            ))
        value = cast(list[Any], value)
        args = get_args(T)
        if len(args) != 1:
            raise TypeError('Expected list type to have exactly one parameter')
        return [ _value(args[0], item) for item in value ]

    def _dict(T : type, value : Any) -> Any:
        if not isinstance(value, dict):
            raise TypeError(
                'Expected a type of dict[str, T] '
                'but got %s for value \"%s\"' % (
                type(value).__name__, str(value)
            ))
        value = cast(dict[Any, Any], value)
        args = get_args(T)
        if len(args) != 2:
            raise TypeError('Expected dict type to have exactly two parameters')
        if args[0] != str:
            raise TypeError(
                'Expected type to be str but got %s' % (
                args[0].__name__
            ))
        for key in value.keys():
            if isinstance(key, str): continue
            raise TypeError(
                'Expected key %s to be of type %s but got %s' % (
                key, str.__name__, type(key).__name__
            ))
        return { k: _value(args[1], v) for k, v in value.items() }

    return _document(T, value)
