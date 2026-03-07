from __future__ import annotations

from datetime import date

from app.providers.dart.corp_codes import parse_corp_code_xml_bytes


def test_parse_corp_code_xml_bytes():
    xml_bytes = b"""
    <result>
      <list>
        <corp_code>00126380</corp_code>
        <corp_name>\xec\x82\xbc\xec\x84\xb1\xec\xa0\x84\xec\x9e\x90</corp_name>
        <stock_code>005930</stock_code>
        <modify_date>20240101</modify_date>
      </list>
      <list>
        <corp_code>00999999</corp_code>
        <corp_name>\xeb\xb9\x84\xec\x83\x81\xec\x9e\xa5</corp_name>
        <stock_code></stock_code>
        <modify_date>20240202</modify_date>
      </list>
    </result>
    """

    frame = parse_corp_code_xml_bytes(xml_bytes)

    assert len(frame) == 2
    assert frame.loc[0, "corp_code"] == "00126380"
    assert frame.loc[0, "stock_code"] == "005930"
    assert frame.loc[0, "modify_date"] == date(2024, 1, 1)
    assert frame.loc[1, "stock_code"] is None
