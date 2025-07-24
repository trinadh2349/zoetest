# ZOE File Generator

## Overview

`zoe.py` is a Python script designed to generate ZOE files for batch processing, supporting both NEW and DELTA modes. It connects to databases, processes records (optionally in parallel), and outputs formatted files for downstream systems.

- **NEW mode:** Generates a new ZOE file from scratch.
- **DELTA mode:** Compares two ZOE files and outputs only the differences.

## Main Features
- Multi-threaded data collection (NEW mode)
- File comparison and delta output (DELTA mode)
- Configurable via YAML config file and command-line arguments
- Output files are written to the specified directory

## Usage

### Running the Script

```
python zoe.py --MODE NEW --CONFIG_FILE_PATH <config.yaml> --OUTPUT_FILE_PATH <output_dir> --OUTPUT_FILE_NAME <output_file> ...
```

- For DELTA mode, also provide `--OLD_ZOE_FILE` and `--NEW_ZOE_FILE` arguments.
- See the `parse_args` function in `zoe.py` for all available arguments.

### Output Files
- **NEW mode:** The output file (e.g., `zoe_report_new.txt`) contains all records in the required format.
- **DELTA mode:** The output file (e.g., `zoe_report_delta.txt`) contains only the changed or added records compared to the old file.

## Testing

### Running Unit Tests

This project uses `pytest` for unit testing. To run the tests:

```
pip install pytest
pytest test_zoe.py
```

- The tests will create output files in the current directory (e.g., `zoe_report_new.txt`, `zoe_report_delta.txt`).
- Tests use fixtures and mocks to simulate database and config dependencies.

### Test Structure
- `conftest.py`: Provides fixtures for fake arguments and script data for both NEW and DELTA modes.
- `test_zoe.py`: Contains tests for the main workflow (`run`, `process_new_mode`, `process_delta_mode`) and for key formatting functions (`build_detail_record`, `build_header_record`, `build_trailer_record`).
- Output files are checked for correct headers and content.

## Dependencies
- Python 3.8+
- `pytest`
- `unittest.mock` (standard library)
- `pyodbc`, `oracledb` (for real database connections, but are mocked in tests)

## Notes
- The script expects a valid YAML config file for real runs.
- For testing, a dummy config is used.
- Output files are overwritten if they already exist.

## License
MIT