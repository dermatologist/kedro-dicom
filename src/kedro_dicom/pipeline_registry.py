"""Project pipelines."""
from typing import Dict

from kedro.pipeline import Pipeline, pipeline

from kedro_dicom.pipelines.preprocess import pipeline as de

def register_pipelines() -> Dict[str, Pipeline]:
    """Register the project's pipelines.

    Returns:
        A mapping from a pipeline name to a ``Pipeline`` object.
    """
    de_pipeline = de.create_pipeline()

    return {
        "de": de_pipeline,
        "__default__": de_pipeline,
    }
