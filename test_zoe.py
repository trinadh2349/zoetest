import os
import pytest
from pathlib import Path
from unittest.mock import patch, MagicMock

from zoe import run, process_new_mode, process_delta_mode

def test_run_new_mode(script_data_new, mocker):
    apwx = script_data_new.apwx
    mocker.patch("zoe.initialize", return_value=script_data_new)
    mocker.patch("zoe.collect_zoe_records_multithreaded", return_value=["A|B|C|1|2|3|4|5|6|7|8|9|10|11|12|13|14|15|16|17|18|19|20|21|22|23|24|25|26|27|28|29|30|31|32|33|34|35|36|37|38|39|40|41|42|43|44|45|46|47|48|49|50|51|52|53|54|55|56"])
    output_file = Path(apwx.args.OUTPUT_FILE_PATH) / apwx.args.OUTPUT_FILE_NAME
    if output_file.exists():
        output_file.unlink()
    result = run(apwx)
    assert result is True
    assert output_file.exists()
    with open(output_file, "r") as f:
        lines = [line.strip() for line in f.readlines() if line.strip()]
    assert any("CDE0380" in line for line in lines)  # Header check
    assert any("6|A|03|FTF|1|A|B|C" in line for line in lines)  # Detail check


def test_run_delta_mode(script_data_delta, mocker, tmp_path):
    apwx = script_data_delta.apwx
    # Prepare old and new ZOE files
    old_file = Path(apwx.args.OLD_ZOE_FILE)
    new_file = Path(apwx.args.NEW_ZOE_FILE)
    old_file.write_text("6|A|03|FTF|1|A|B|C|1|2|3|4|5|6|7|8|9|10|11|12|13|14|15|16|17|18|19|20|21|22|23|24|25|26|27|28|29|30|31|32|33|34|35|36|37|38|39|40|41|42|43|44|45|46|47|48|49|50|51|52|53|54|55|56\n")
    new_file.write_text("6|A|03|FTF|1|A|B|C|1|2|3|4|5|6|7|8|9|10|11|12|13|14|15|16|17|18|19|20|21|22|23|24|25|26|27|28|29|30|31|32|33|34|35|36|37|38|39|40|41|42|43|44|45|46|47|48|49|50|51|52|53|54|55|56\n6|A|03|FTF|2|X|Y|Z|1|2|3|4|5|6|7|8|9|10|11|12|13|14|15|16|17|18|19|20|21|22|23|24|25|26|27|28|29|30|31|32|33|34|35|36|37|38|39|40|41|42|43|44|45|46|47|48|49|50|51|52|53|54|55|56\n")
    mocker.patch("zoe.get_zoe_file_hash", side_effect=[({'C': 'A|B|C|1|2|3|4|5|6|7|8|9|10|11|12|13|14|15|16|17|18|19|20|21|22|23|24|25|26|27|28|29|30|31|32|33|34|35|36|37|38|39|40|41|42|43|44|45|46|47|48|49|50|51|52|53|54|55|56'}, 1), ({'C': 'A|B|C|1|2|3|4|5|6|7|8|9|10|11|12|13|14|15|16|17|18|19|20|21|22|23|24|25|26|27|28|29|30|31|32|33|34|35|36|37|38|39|40|41|42|43|44|45|46|47|48|49|50|51|52|53|54|55|56', 'Z': 'X|Y|Z|1|2|3|4|5|6|7|8|9|10|11|12|13|14|15|16|17|18|19|20|21|22|23|24|25|26|27|28|29|30|31|32|33|34|35|36|37|38|39|40|41|42|43|44|45|46|47|48|49|50|51|52|53|54|55|56'}, 2)])
    output_file = Path(apwx.args.OUTPUT_FILE_PATH) / apwx.args.OUTPUT_FILE_NAME
    if output_file.exists():
        output_file.unlink()
    result = run(apwx)
    assert result is True
    assert output_file.exists()
    with open(output_file, "r") as f:
        lines = [line.strip() for line in f.readlines() if line.strip()]
    assert any("CDE0380" in line for line in lines)  # Header check
    assert any("6|A|03|FTF" in line for line in lines)  # Detail check


def test_process_new_mode_creates_file(script_data_new, mocker):
    apwx = script_data_new.apwx
    mocker.patch("zoe.collect_zoe_records_multithreaded", return_value=["A|B|C|1|2|3|4|5|6|7|8|9|10|11|12|13|14|15|16|17|18|19|20|21|22|23|24|25|26|27|28|29|30|31|32|33|34|35|36|37|38|39|40|41|42|43|44|45|46|47|48|49|50|51|52|53|54|55|56"])
    output_file = Path(apwx.args.OUTPUT_FILE_PATH) / apwx.args.OUTPUT_FILE_NAME
    if output_file.exists():
        output_file.unlink()
    result = process_new_mode(apwx, script_data_new, str(output_file))
    assert result is True
    assert output_file.exists()
    with open(output_file, "r") as f:
        lines = [line.strip() for line in f.readlines() if line.strip()]
    assert any("CDE0380" in line for line in lines)
    assert any("6|A|03|FTF|1|A|B|C" in line for line in lines)


def test_process_delta_mode_creates_file(script_data_delta, mocker):
    apwx = script_data_delta.apwx
    output_file = Path(apwx.args.OUTPUT_FILE_PATH) / apwx.args.OUTPUT_FILE_NAME
    if output_file.exists():
        output_file.unlink()
    mocker.patch("zoe.get_zoe_file_hash", side_effect=[({'C': 'A|B|C|1|2|3|4|5|6|7|8|9|10|11|12|13|14|15|16|17|18|19|20|21|22|23|24|25|26|27|28|29|30|31|32|33|34|35|36|37|38|39|40|41|42|43|44|45|46|47|48|49|50|51|52|53|54|55|56'}, 1), ({'C': 'A|B|C|1|2|3|4|5|6|7|8|9|10|11|12|13|14|15|16|17|18|19|20|21|22|23|24|25|26|27|28|29|30|31|32|33|34|35|36|37|38|39|40|41|42|43|44|45|46|47|48|49|50|51|52|53|54|55|56', 'Z': 'X|Y|Z|1|2|3|4|5|6|7|8|9|10|11|12|13|14|15|16|17|18|19|20|21|22|23|24|25|26|27|28|29|30|31|32|33|34|35|36|37|38|39|40|41|42|43|44|45|46|47|48|49|50|51|52|53|54|55|56'}, 2)])
    result = process_delta_mode(apwx, str(output_file))
    assert result is True
    assert output_file.exists()
    with open(output_file, "r") as f:
        lines = [line.strip() for line in f.readlines() if line.strip()]
    assert any("CDE0380" in line for line in lines)
    assert any("6|A|03|FTF" in line for line in lines)