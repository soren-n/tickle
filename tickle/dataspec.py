# External module dependencies
from dataclasses import is_dataclass
from typing import cast, get_origin, get_args, Any, Union, List, Dict
from inspect import signature

###############################################################################
# Type
###############################################################################
DataspecValue = Union[
    int, float, str,
    'Dataspec'
]
Dataspec = Union[
    List['DataspecValue'],
    Dict[str, 'DataspecValue']
]

###############################################################################
# Functions
###############################################################################
def decode(T : type, value : Any) -> Any:
    def _value(T : type, value : Any) -> Any:
        if T in [str, int, float]: return _simple(T, value)
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
        for l, t in signature(T).parameters.items():
            if l not in value:
                raise TypeError('Expected dict to define field %s' % l)
            result[l] = _value(t.annotation, value[l])
        return T(**result)

    def _simple(T : type, value : Any) -> Any:
            S = type(value)
            if T == S: return value
            raise TypeError(
                'Expected a type of %s but got %s for value \"%s\"' % (
                T.__name__, S.__name__, str(value)
            ))

    def _list(T : type, value : Any) -> List[Any]:
        if not isinstance(value, list):
            raise TypeError(
                'Expected a type of List[T] but got %s for value \"%s\"' % (
                type(value).__name__, str(value)
            ))
        value = cast(List[Any], value)
        args = get_args(T)
        if len(args) != 1:
            raise TypeError(
                'Expected list type to have exactly one parameter'
            )
        return [ _value(args[0], item) for item in value ]

    def _dict(T : type, value : Any) -> Dict[Any, Any]:
        if not isinstance(value, dict):
            raise TypeError(
                'Expected a type of dict but got %s' % (
                type(value).__name__
            ))
        value = cast(Dict[Any, Any], value)
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
        return { k: _value(args[1], v) for k, v in value.items() }

    return _document(T, value)

def encode(T : type, value : Any) -> Dataspec:
    def _value(T : type, value : Any) -> Any:
        if T in [str, int, float]: return _simple(T, value)
        return _document(T, value)

    def _composite(T : type, value : Any) -> Any:
        origin = get_origin(T)
        if origin != None:
            if origin == list: return _list(T, value)
            if origin == dict: return _dict(T, value)
        raise TypeError('Unsupported dataspec type %s' % T.__name__)

    def _document(T : type, value : Any) -> Any:
        if not is_dataclass(T): return _composite(T, value)
        result : Dict[str, Any] = {}
        for l, t in signature(T).parameters.items():
            if l not in value.__dict__:
                raise TypeError('Expected dataclass to define field %s' % l)
            result[l] = _value(t.annotation, value.__dict__[l])
        return result

    def _simple(T : type, value : Any) -> Any:
        S = type(value)
        if T == S: return value
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
        value = cast(List[Any], value)
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
        value = cast(Dict[Any, Any], value)
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
