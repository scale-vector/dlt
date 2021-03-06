import io

from dlt.common.dataset_writers import write_insert_values, escape_redshift_literal, escape_redshift_identifier

from tests.common.utils import load_json_case

def test_simple_insert_writer() -> None:
    rows = load_json_case("simple_row")
    with io.StringIO() as f:
        write_insert_values(f, rows, rows[0].keys())
        lines = f.getvalue().split("\n")
    assert lines[0].startswith("INSERT INTO {}")
    assert '","'.join(rows[0].keys()) in lines[0]
    assert lines[1] == "VALUES"
    assert len(lines) == 4


def test_unicode_insert_writer() -> None:
    rows = load_json_case("weird_rows")
    with io.StringIO() as f:
        write_insert_values(f, rows, rows[0].keys())
        lines = f.getvalue().split("\n")
    assert lines[2].endswith("', NULL''); DROP SCHEMA Public --'),")
    assert lines[3].endswith("'イロハニホヘト チリヌルヲ ''ワカヨタレソ ツネナラム'),")
    assert lines[4].endswith("'ऄअआइ''ईउऊऋऌऍऎए');")


def test_string_literal_escape() -> None:
    assert escape_redshift_literal(", NULL'); DROP TABLE --") == "', NULL''); DROP TABLE --'"
    assert escape_redshift_literal(", NULL');\n DROP TABLE --") == "', NULL'');\n DROP TABLE --'"
    assert escape_redshift_literal(", NULL);\n DROP TABLE --") == "', NULL);\n DROP TABLE --'"
    assert escape_redshift_literal(", NULL);\\n DROP TABLE --\\") == "', NULL);\\\\n DROP TABLE --\\\\'"


def test_identifier_escape() -> None:
    assert escape_redshift_identifier(", NULL'); DROP TABLE\" -\\-") == '", NULL\'); DROP TABLE"" -\\\\-"'


def test_string_escape_unicode() -> None:
    # test on some unicode characters
    assert escape_redshift_literal(", NULL);\n DROP TABLE --") == "', NULL);\n DROP TABLE --'"
    assert escape_redshift_literal("イロハニホヘト チリヌルヲ ワカヨタレソ ツネナラム") == "'イロハニホヘト チリヌルヲ ワカヨタレソ ツネナラム'"
    assert escape_redshift_identifier("ąćł\"") == '"ąćł"""'
    assert escape_redshift_identifier("イロハニホヘト チリヌルヲ \"ワカヨタレソ ツネナラム") == '"イロハニホヘト チリヌルヲ ""ワカヨタレソ ツネナラム"'
