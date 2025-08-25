from xmldiff import main as xmldiff
from logger_factory import create_logger
from protocols.app_logger import AppLogger
from content_formatter import ContentFormatter
import pytest


def test_format_xml_to_json():
    xml_payload = "<root><element>value</element></root>"
    result = ContentFormatter.format_data(xml_payload, "xml", "json")
    assert '"element": "value"' in result

def test_format_json_to_xml():
    json_payload = '{"element": "value"}'
    result = ContentFormatter.format_data(json_payload, "json", "xml")
    assert "<element>value</element>" in result

def test_map_xml_to_xml():
    logger:AppLogger = create_logger("test_logger")
    logger.info("Starting test_map_xml_to_xml")
    source_xml_payload = "<pro_trader><trade><trader>JCOOK</trader><trade_id>10090</trade_id><ccy>SEK</ccy></trade></pro_trader>"
    result = ContentFormatter._map_xml_to_xml(source_xml_payload)
    expected = "<common_trade><currency_code>SEK</currency_code></common_trade>"
    diff = xmldiff.diff_texts(result, expected)
    assert not diff  # Should be empty if XMLs are equivalent

def test_invalid_xml_to_json_raises():
    with pytest.raises(ValueError):
        ContentFormatter.format_data("invalid", "xml", "json")

def test_invalid_json_to_xml_raises():
    with pytest.raises(ValueError):
        ContentFormatter.format_data("invalid", "json", "xml")