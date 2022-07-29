import pytest

from dlt.common.normalizers.names.snake_case import normalize_column_name, normalize_table_name, normalize_make_dataset_name


def test_fix_field_name() -> None:
    assert normalize_column_name("event_value") == "event_value"
    assert normalize_column_name("event value") == "event_value"
    assert normalize_column_name("event-.!:<>value") == "event_value"
    # prefix leading digits
    assert normalize_column_name("1event_n'") == "_1event_n_"
    assert normalize_column_name("123event_n'") == "_123event_n_"
    # all lowercase and converted to snake
    assert normalize_column_name("123BaNaNa") == "_123_ba_na_na"
    # consecutive capital letters
    assert normalize_column_name("BANANA") == "banana"
    assert normalize_column_name("BAN_ANA") == "ban_ana"
    assert normalize_column_name("BANaNA") == "ba_na_na"


def test_normalizes_underscores() -> None:
    assert normalize_column_name("event__value_value2____") == "event_value_value2_"
    assert normalize_table_name("e_vent__value_value2____") == "e_vent__value_value2__"


def test_normalize_make_dataset_name() -> None:
    # second part is not normalized, a proper schema name is assumed to be used
    assert normalize_make_dataset_name("BAN_ANA", "BANANA") == "ban_ana_BANANA"
    assert normalize_make_dataset_name("BAN_ANA", "") == "ban_ana"
    assert normalize_make_dataset_name("BAN_ANA", None) == "ban_ana"

    with pytest.raises(ValueError):
        normalize_make_dataset_name("", "BAN_ANA")
    with pytest.raises(ValueError):
        normalize_make_dataset_name(None, "BAN_ANA")
