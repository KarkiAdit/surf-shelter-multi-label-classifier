from setuptools import setup, find_packages

# Read the dependencies from requirements file
def read_requirements():
    with open("requirements.txt") as f:
        return f.read().splitlines()

setup(
    name="surf_shelter_multi_label_training_pkg",
    version="0.1",
    packages=find_packages(),
    python_requires=">=3.7",
    install_requires=read_requirements(),
)
