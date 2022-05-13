"""
This module contains an example test.

Tests should be placed in ``src/tests``, in modules that mirror your
project's structure, and in files named test_*.py. They are simply functions
named ``test_*`` which test a unit of logic.

To run the tests, run ``kedro test`` from the project root directory.
"""

from pathlib import Path

import pytest

from kedro.framework.project import settings
from kedro.config import ConfigLoader
from kedro.framework.context import KedroContext
from kedro.framework.hooks import _create_hook_manager
from kedro.io import PartitionedDataSet
from kedro_dicom.pipelines.preprocess.nodes import preprocess_dicom
import numpy as np

@pytest.fixture
def config_loader():
    return ConfigLoader(conf_source=str(Path.cwd() / settings.CONF_SOURCE))


@pytest.fixture
def project_context(config_loader):
    return KedroContext(
        package_name="kedro_dicom",
        project_path=Path.cwd(),
        config_loader=config_loader,
        hook_manager=_create_hook_manager(),
    )


# The tests below are here for the demonstration purpose
# and should be replaced with the ones testing the project
# functionality
class TestProjectContext:
    def test_project_path(self, project_context):
        assert project_context.project_path == Path.cwd()

    def test_dicom_read(self, project_context):
        dataset = {
            "type": "kedro_dicom.io.datasets.dicom_dataset.DICOMDataSet",
        }
        path = 'data/01_raw/test_imageset'
        filename_suffix =  ".dcm"
        data_set = PartitionedDataSet(
            dataset=dataset, path=path, filename_suffix=filename_suffix)
        reloaded = data_set.load()
        data = preprocess_dicom(reloaded)
        print(data[0].shape)
        for key, value in data[1].items():
            print(key, value)
            assert (value.dtype == np.int16)
        #assert data['_cat_lazy_sleep_1']['labels'] == ['cat', 'lazy', 'sleep']
