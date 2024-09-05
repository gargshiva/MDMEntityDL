import pytest

from lib.config_loader import load_app_config
from lib.utils import Utils


@pytest.fixture(scope='session')
def spark():
    return Utils.get_spark_session('LOCAL')



def test_config_load():
    conf = load_app_config('LOCAL')
    print(conf)
    assert conf['enable.hive'] == 'False'
