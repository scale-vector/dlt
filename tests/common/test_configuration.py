from os import environ
from typing import Any, Dict, List, NewType, Optional, Tuple

import pytest

from dlt.common.configuration import (
    RunConfiguration, ConfigEntryMissingException, ConfigFileNotFoundException,
    ConfigEnvValueCannotBeCoercedException, utils)
from dlt.common.configuration.utils import (_coerce_single_value, IS_DEVELOPMENT_CONFIG_KEY,
                                                           _get_config_attrs_with_hints, TSecretValue,
                                                           is_direct_descendant, make_configuration)
from tests.utils import preserve_environ

# used to test version
__version__ = "1.0.5"

IS_DEVELOPMENT_CONFIG = 'DEBUG'
NONE_CONFIG_VAR = 'NoneConfigVar'
COERCIONS = {
    'STR_VAL': 'test string',
    'INT_VAL': 12345,
    'BOOL_VAL': True,
    'LIST_VAL': [1, "2", [3]],
    'DICT_VAL': {
        'a': 1,
        "b": "2"
    },
    'TUPLE_VAL': (1, 2, '7'),
    'SET_VAL': {1, 2, 3},
    'BYTES_VAL': b'Hello World!',
    'FLOAT_VAL': 1.18927,
    'ANY_VAL': "function() {}",
    'NONE_VAL': "none",
    'COMPLEX_VAL': {
        "_": (1440, ["*"], []),
        "change-email": (560, ["*"], [])
    }
}

INVALID_COERCIONS = {
    # 'STR_VAL': 'test string',  # string always OK
    'INT_VAL': "a12345",
    'BOOL_VAL': "Yes",  # bool overridden by string - that is the most common problem
    'LIST_VAL': {1, "2", 3.0},
    'DICT_VAL': "{'a': 1, 'b', '2'}",
    'TUPLE_VAL': [1, 2, '7'],
    'SET_VAL': [1, 2, 3],
    'BYTES_VAL': 'Hello World!',
    'FLOAT_VAL': "invalid"
}

EXCEPTED_COERCIONS = {
    # allows to use int for float
    'FLOAT_VAL': 10,
    # allows to use float for str
    'STR_VAL': 10.0
}

COERCED_EXCEPTIONS = {
    # allows to use int for float
    'FLOAT_VAL': 10.0,
    # allows to use float for str
    'STR_VAL': "10.0"
}


class SimpleConfiguration(RunConfiguration):
    PIPELINE_NAME: str = "Some Name"


class WrongConfiguration(RunConfiguration):
    PIPELINE_NAME: str = "Some Name"
    NoneConfigVar = None
    LOG_COLOR: bool = True


class SecretConfiguration(RunConfiguration):
    PIPELINE_NAME: str = "secret"
    SECRET_VALUE: TSecretValue = None


class SecretKubeConfiguration(RunConfiguration):
    PIPELINE_NAME: str = "secret kube"
    SECRET_KUBE: TSecretValue = None


class TestCoercionConfiguration(RunConfiguration):
    PIPELINE_NAME: str = "Some Name"
    STR_VAL: str = None
    INT_VAL: int = None
    BOOL_VAL: bool = None
    LIST_VAL: list = None  # type: ignore
    DICT_VAL: dict = None  # type: ignore
    TUPLE_VAL: tuple = None  # type: ignore
    BYTES_VAL: bytes = None
    SET_VAL: set = None  # type: ignore
    FLOAT_VAL: float = None
    ANY_VAL: Any = None
    NONE_VAL = None
    COMPLEX_VAL: Dict[str, Tuple[int, List[str], List[str]]] = None


class VeryWrongConfiguration(WrongConfiguration):
    PIPELINE_NAME: str = "Some Name"
    STR_VAL: str = ""
    INT_VAL: int = None
    LOG_COLOR: str = "1"  # type: ignore


class ConfigurationWithOptionalTypes(RunConfiguration):
    PIPELINE_NAME: str = "Some Name"

    STR_VAL: Optional[str] = None
    INT_VAL: Optional[int] = None
    BOOL_VAL: bool = True


class ProdConfigurationWithOptionalTypes(ConfigurationWithOptionalTypes):
    PROD_VAL: str = "prod"


class MockProdConfiguration(RunConfiguration):
    PIPELINE_NAME: str = "comp"


class MockProdConfigurationVar(RunConfiguration):
    PIPELINE_NAME: str = "comp"


LongInteger = NewType("LongInteger", int)
FirstOrderStr = NewType("FirstOrderStr", str)
SecondOrderStr = NewType("SecondOrderStr", FirstOrderStr)


@pytest.fixture(scope="function")
def environment() -> Any:
    environ.clear()
    return environ


def test_run_configuration_gen_name(environment: Any) -> None:
    C = make_configuration(RunConfiguration, RunConfiguration)
    assert C.PIPELINE_NAME.startswith("dlt_")


def test_configuration_to_dict(environment: Any) -> None:
    expected_dict = {
        'CONFIG_FILES_STORAGE_PATH': '_storage/config/%s',
        'IS_DEVELOPMENT_CONFIG': True,
        'LOG_FORMAT': '{asctime}|[{levelname:<21}]|{process}|{name}|{filename}|{funcName}:{lineno}|{message}',
        'LOG_LEVEL': 'DEBUG',
        'PIPELINE_NAME': 'secret',
        'PROMETHEUS_PORT': None,
        'REQUEST_TIMEOUT': (15, 300),
        'SECRET_VALUE': None,
        'SENTRY_DSN': None
    }
    assert SecretConfiguration.as_dict() == {k.lower():v for k,v in expected_dict.items()}
    assert SecretConfiguration.as_dict(lowercase=False) == expected_dict

    environment["SECRET_VALUE"] = "secret"
    C = make_configuration(SecretConfiguration, SecretConfiguration)
    d = C.as_dict(lowercase=False)
    expected_dict["_VERSION"] = d["_VERSION"]
    expected_dict["SECRET_VALUE"] = "secret"
    assert d == expected_dict


def test_configuration_rise_exception_when_config_is_not_complete() -> None:
    with pytest.raises(ConfigEntryMissingException) as config_entry_missing_exception:
        keys = _get_config_attrs_with_hints(WrongConfiguration)
        utils._is_config_bounded(WrongConfiguration, keys)

    assert 'NoneConfigVar' in config_entry_missing_exception.value.missing_set


def test_optional_types_are_not_required() -> None:
    # this should not raise an exception
    keys = _get_config_attrs_with_hints(ConfigurationWithOptionalTypes)
    utils._is_config_bounded(ConfigurationWithOptionalTypes, keys)
    # make optional config
    make_configuration(ConfigurationWithOptionalTypes, ConfigurationWithOptionalTypes)
    # make config with optional values
    make_configuration(
        ProdConfigurationWithOptionalTypes,
        ProdConfigurationWithOptionalTypes,
        initial_values={"INT_VAL": None}
    )


def test_configuration_apply_adds_environment_variable_to_config(environment: Any) -> None:
    environment[NONE_CONFIG_VAR] = "Some"

    keys = _get_config_attrs_with_hints(WrongConfiguration)
    utils._apply_environ_to_config(WrongConfiguration, keys)
    utils._is_config_bounded(WrongConfiguration, keys)

    # NoneConfigVar has no hint so value not coerced from string
    assert WrongConfiguration.NoneConfigVar == environment[NONE_CONFIG_VAR]


def test_conf(environment: Any) -> None:
    environment[IS_DEVELOPMENT_CONFIG] = 'True'

    keys = _get_config_attrs_with_hints(SimpleConfiguration)
    utils._apply_environ_to_config(SimpleConfiguration, keys)
    utils._is_config_bounded(SimpleConfiguration, keys)

    # value will be coerced to bool
    assert RunConfiguration.IS_DEVELOPMENT_CONFIG is True


def test_find_all_keys() -> None:
    keys = _get_config_attrs_with_hints(VeryWrongConfiguration)
    # assert hints and types: NoneConfigVar has no type hint and LOG_COLOR had it hint overwritten in derived class
    assert set({'STR_VAL': str, 'INT_VAL': int, 'NoneConfigVar': None, 'LOG_COLOR': str}.items()).issubset(keys.items())


def test_coercions(environment: Any) -> None:
    for key, value in COERCIONS.items():
        environment[key] = str(value)

    keys = _get_config_attrs_with_hints(TestCoercionConfiguration)
    utils._apply_environ_to_config(TestCoercionConfiguration, keys)
    utils._is_config_bounded(TestCoercionConfiguration, keys)

    for key in COERCIONS:
        assert getattr(TestCoercionConfiguration, key) == COERCIONS[key]


def test_invalid_coercions(environment: Any) -> None:
    config_keys = _get_config_attrs_with_hints(TestCoercionConfiguration)
    for key, value in INVALID_COERCIONS.items():
        try:
            environment[key] = str(value)
            utils._apply_environ_to_config(TestCoercionConfiguration, config_keys)
        except ConfigEnvValueCannotBeCoercedException as coerc_exc:
            # must fail excatly on expected value
            if coerc_exc.attr_name != key:
                raise
            # overwrite with valid value and go to next env
            environment[key] = str(COERCIONS[key])
            continue
        raise AssertionError("%s was coerced with %s which is invalid type" % (key, value))


def test_excepted_coercions(environment: Any) -> None:
    config_keys = _get_config_attrs_with_hints(TestCoercionConfiguration)
    for k, v in EXCEPTED_COERCIONS.items():
        environment[k] = str(v)
        utils._apply_environ_to_config(TestCoercionConfiguration, config_keys)
    for key in EXCEPTED_COERCIONS:
        assert getattr(TestCoercionConfiguration, key) == COERCED_EXCEPTIONS[key]


def test_development_config_detection(environment: Any) -> None:
    # default is true
    assert utils._is_development_config()
    environment[IS_DEVELOPMENT_CONFIG_KEY] = "False"
    # explicit values
    assert not utils._is_development_config()
    environment[IS_DEVELOPMENT_CONFIG_KEY] = "True"
    assert utils._is_development_config()
    # raise exception on env value that cannot be coerced to bool
    with pytest.raises(ConfigEnvValueCannotBeCoercedException):
        environment[IS_DEVELOPMENT_CONFIG_KEY] = "NONBOOL"
        utils._is_development_config()


def test_make_configuration(environment: Any) -> None:
    # fill up configuration
    environment['INT_VAL'] = "1"
    # default is true
    assert is_direct_descendant(utils.make_configuration(WrongConfiguration, VeryWrongConfiguration), WrongConfiguration)
    environment[IS_DEVELOPMENT_CONFIG_KEY] = "False"
    assert is_direct_descendant(utils.make_configuration(WrongConfiguration, VeryWrongConfiguration), VeryWrongConfiguration)
    environment[IS_DEVELOPMENT_CONFIG_KEY] = "True"
    assert is_direct_descendant(utils.make_configuration(WrongConfiguration, VeryWrongConfiguration), WrongConfiguration)


def test_auto_derivation(environment: Any) -> None:
    # make_configuration auto derives a type and never modifies the original type
    environment['SECRET_VALUE'] = "1"
    C = utils.make_configuration(SecretConfiguration, SecretConfiguration)
    # auto derived type holds the value
    assert C.SECRET_VALUE == "1"
    # base type is untouched
    assert SecretConfiguration.SECRET_VALUE is None
    # type name is derived
    assert C.__name__.startswith("SecretConfiguration_")


def test_initial_values(environment: Any) -> None:
    # initial values will be overridden from env
    environment["PIPELINE_NAME"] = "env name"
    environment["CREATED_VAL"] = "12837"
    # set initial values and allow partial config
    C = make_configuration(TestCoercionConfiguration, TestCoercionConfiguration,
        {"PIPELINE_NAME": "initial name", "NONE_VAL": type(environment), "CREATED_VAL": 878232, "BYTES_VAL": b"str"},
        accept_partial=True
    )
    # from env
    assert C.PIPELINE_NAME == "env name"
    # from initial
    assert C.BYTES_VAL == b"str"
    assert C.NONE_VAL == type(environment)
    # new prop overridden from env
    assert environment["CREATED_VAL"] == "12837"


def test_finds_version(environment: Any) -> None:
    global __version__

    v = __version__
    C = utils.make_configuration(SimpleConfiguration, SimpleConfiguration)
    assert C._VERSION == v
    try:
        del globals()["__version__"]
        # C is a type, not instance and holds the _VERSION from previous extract
        delattr(C, "_VERSION")
        C = utils.make_configuration(SimpleConfiguration, SimpleConfiguration)
        assert not hasattr(C, "_VERSION")
    finally:
        __version__ = v


def test_secret(environment: Any) -> None:
    with pytest.raises(ConfigEntryMissingException):
        utils.make_configuration(SecretConfiguration, SecretConfiguration)
    environment['SECRET_VALUE'] = "1"
    C = utils.make_configuration(SecretConfiguration, SecretConfiguration)
    assert C.SECRET_VALUE == "1"
    # mock the path to point to secret storage
    # from dlt.common.configuration import config_utils
    path = utils.SECRET_STORAGE_PATH
    del environment['SECRET_VALUE']
    try:
        # must read a secret file
        utils.SECRET_STORAGE_PATH = "./tests/common/cases/%s"
        C = utils.make_configuration(SecretConfiguration, SecretConfiguration)
        assert C.SECRET_VALUE == "BANANA"

        # set some weird path, no secret file at all
        del environment['SECRET_VALUE']
        utils.SECRET_STORAGE_PATH = "!C:\\PATH%s"
        with pytest.raises(ConfigEntryMissingException):
            utils.make_configuration(SecretConfiguration, SecretConfiguration)

        # set env which is a fallback for secret not as file
        environment['SECRET_VALUE'] = "1"
        C = utils.make_configuration(SecretConfiguration, SecretConfiguration)
        assert C.SECRET_VALUE == "1"
    finally:
        utils.SECRET_STORAGE_PATH = path


def test_secret_kube_fallback(environment: Any) -> None:
    path = utils.SECRET_STORAGE_PATH
    try:
        utils.SECRET_STORAGE_PATH = "./tests/common/cases/%s"
        C = utils.make_configuration(SecretKubeConfiguration, SecretKubeConfiguration)
        # all unix editors will add x10 at the end of file, it will be preserved
        assert C.SECRET_KUBE == "kube\n"
        # we propagate secrets back to environ and strip the whitespace
        assert environment['SECRET_KUBE'] == "kube"
    finally:
        utils.SECRET_STORAGE_PATH = path


def test_configuration_must_be_subclass_of_prod(environment: Any) -> None:
    # fill up configuration
    environment['INT_VAL'] = "1"
    # prod must inherit from config
    with pytest.raises(AssertionError):
        # VeryWrongConfiguration does not descend inherit from ConfigurationWithOptionalTypes so it cannot be production config of it
        utils.make_configuration(ConfigurationWithOptionalTypes, VeryWrongConfiguration)


def test_coerce_values() -> None:
    with pytest.raises(ConfigEnvValueCannotBeCoercedException):
        _coerce_single_value("key", "some string", int)
    assert _coerce_single_value("key", "some string", str) == "some string"
    # Optional[str] has type object, mypy will never work properly...
    assert _coerce_single_value("key", "some string", Optional[str]) == "some string"  # type: ignore

    assert _coerce_single_value("key", "234", int) == 234
    assert _coerce_single_value("key", "234", Optional[int]) == 234  # type: ignore

    # check coercions of NewTypes
    assert _coerce_single_value("key", "test str X", FirstOrderStr) == "test str X"
    assert _coerce_single_value("key", "test str X", Optional[FirstOrderStr]) == "test str X"  # type: ignore
    assert _coerce_single_value("key", "test str X", Optional[SecondOrderStr]) == "test str X"  # type: ignore
    assert _coerce_single_value("key", "test str X", SecondOrderStr) == "test str X"
    assert _coerce_single_value("key", "234", LongInteger) == 234
    assert _coerce_single_value("key", "234", Optional[LongInteger]) == 234  # type: ignore
    # this coercion should fail
    with pytest.raises(ConfigEnvValueCannotBeCoercedException):
        _coerce_single_value("key", "some string", LongInteger)
    with pytest.raises(ConfigEnvValueCannotBeCoercedException):
        _coerce_single_value("key", "some string", Optional[LongInteger])  # type: ignore


def test_configuration_files_prod_path(environment: Any) -> None:
    environment[IS_DEVELOPMENT_CONFIG_KEY] = "True"
    C = utils.make_configuration(MockProdConfiguration, MockProdConfiguration)
    assert C.CONFIG_FILES_STORAGE_PATH == "_storage/config/%s"

    environment[IS_DEVELOPMENT_CONFIG_KEY] = "False"
    C = utils.make_configuration(MockProdConfiguration, MockProdConfiguration)
    assert C.IS_DEVELOPMENT_CONFIG is False
    assert C.CONFIG_FILES_STORAGE_PATH == "/run/config/%s"


def test_configuration_files(environment: Any) -> None:
    # overwrite config file paths
    environment[IS_DEVELOPMENT_CONFIG_KEY] = "False"
    environment["CONFIG_FILES_STORAGE_PATH"] = "./tests/common/cases/schemas/ev1/%s"
    C = utils.make_configuration(MockProdConfigurationVar, MockProdConfigurationVar)
    assert C.CONFIG_FILES_STORAGE_PATH == environment["CONFIG_FILES_STORAGE_PATH"]
    assert C.has_configuration_file("hasn't") is False
    assert C.has_configuration_file("event_schema.json") is True
    assert C.get_configuration_file_path("event_schema.json") == "./tests/common/cases/schemas/ev1/event_schema.json"
    with C.open_configuration_file("event_schema.json", "r") as f:
        f.read()
    with pytest.raises(ConfigFileNotFoundException):
        C.open_configuration_file("hasn't", "r")
