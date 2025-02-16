from setuptools import setup, find_packages

setup(
    name="surf_shelter_multi_label_training_pkg",
    version="0.1",
    packages=find_packages(),
    package_data={
        "multi_label_model_trainer": ["warc.paths.gz"],
    },
    python_requires=">=3.7",
)
