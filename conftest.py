import os
import pathlib
import pytest
from dataclasses import dataclass
from unittest.mock import MagicMock

from zoe import AppWorxEnum, get_config, ScriptData

@dataclass
class FakeApwxArgs:
    TNS_SERVICE_NAME: str
    CONFIG_FILE_PATH: str
    OUTPUT_FILE_PATH: str
    OUTPUT_FILE_NAME: str
    TEST_YN: str
    DEBUG_YN: str
    MAX_THREADS: str
    MODE: str
    P2P_SERVER: str
    P2P_SCHEMA: str
    OLD_ZOE_FILE: str = "old_zoe.txt"
    NEW_ZOE_FILE: str = "new_zoe.txt"

@dataclass
class FakeApwx:
    args: FakeApwxArgs
    def db_connect(self, autocommit=False):
        return MagicMock()

TEST_BASE_PATH = pathlib.Path(os.path.dirname(__file__))
CONFIG_PATH = str(TEST_BASE_PATH / "test_config.yaml")

SCRIPT_ARGUMENTS_NEW = {
    str(AppWorxEnum.TNS_SERVICE_NAME): "FAKE_DB",
    str(AppWorxEnum.CONFIG_FILE_PATH): CONFIG_PATH,
    str(AppWorxEnum.OUTPUT_FILE_PATH): str(TEST_BASE_PATH),
    str(AppWorxEnum.OUTPUT_FILE_NAME): "zoe_report_new.txt",
    str(AppWorxEnum.TEST_YN): "Y",
    str(AppWorxEnum.DEBUG_YN): "N",
    str(AppWorxEnum.MAX_THREADS): "1",
    str(AppWorxEnum.MODE): "NEW",
    str(AppWorxEnum.P2P_SERVER): "FAKE_SERVER",
    str(AppWorxEnum.P2P_SCHEMA): "FAKE_SCHEMA",
}

SCRIPT_ARGUMENTS_DELTA = {
    **SCRIPT_ARGUMENTS_NEW,
    str(AppWorxEnum.MODE): "DELTA",
    str(AppWorxEnum.OUTPUT_FILE_NAME): "zoe_report_delta.txt",
    str(AppWorxEnum.OLD_ZOE_FILE): str(TEST_BASE_PATH / "old_zoe.txt"),
    str(AppWorxEnum.NEW_ZOE_FILE): str(TEST_BASE_PATH / "new_zoe.txt"),
}

def new_fake_apwx(script_args: dict) -> FakeApwx:
    return FakeApwx(
        args=FakeApwxArgs(
            TNS_SERVICE_NAME=script_args[str(AppWorxEnum.TNS_SERVICE_NAME)],
            CONFIG_FILE_PATH=str(script_args[str(AppWorxEnum.CONFIG_FILE_PATH)]),
            OUTPUT_FILE_PATH=str(script_args[str(AppWorxEnum.OUTPUT_FILE_PATH)]),
            OUTPUT_FILE_NAME=script_args[str(AppWorxEnum.OUTPUT_FILE_NAME)],
            TEST_YN=script_args[str(AppWorxEnum.TEST_YN)],
            DEBUG_YN=script_args[str(AppWorxEnum.DEBUG_YN)],
            MAX_THREADS=script_args[str(AppWorxEnum.MAX_THREADS)],
            MODE=script_args[str(AppWorxEnum.MODE)],
            P2P_SERVER=script_args[str(AppWorxEnum.P2P_SERVER)],
            P2P_SCHEMA=script_args[str(AppWorxEnum.P2P_SCHEMA)],
            OLD_ZOE_FILE=script_args.get(str(AppWorxEnum.OLD_ZOE_FILE), "old_zoe.txt"),
            NEW_ZOE_FILE=script_args.get(str(AppWorxEnum.NEW_ZOE_FILE), "new_zoe.txt"),
        )
    )

@pytest.fixture(scope="module")
def script_data_new():
    apwx = new_fake_apwx(SCRIPT_ARGUMENTS_NEW)
    config = {"dummy": "value"}  # Replace with actual config structure if needed
    dbh = MagicMock()
    return ScriptData(apwx=apwx, dbh=dbh, config=config)

@pytest.fixture(scope="module")
def script_data_delta():
    apwx = new_fake_apwx(SCRIPT_ARGUMENTS_DELTA)
    config = {"dummy": "value"}  # Replace with actual config structure if needed
    dbh = MagicMock()
    return ScriptData(apwx=apwx, dbh=dbh, config=config)