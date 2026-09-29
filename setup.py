from setuptools import setup, find_packages
from pathlib import Path

this_directory = Path(__file__).parent
long_description = (this_directory / "README.md").read_text(encoding="utf-8")

setup(
    name="dbmeta",
    version="2.0.0",
    packages=find_packages(),
    install_requires=[],
    author="Himanshu Singh",
    author_email="Himanshuclub88@gmail.com",
    description="A pure-Python mini SQL + Python query engine using folder-based  metadeta of a batch job combines to a single table",
    long_description=long_description,
    long_description_content_type="text/markdown",
    url="https://github.com/himanshuclub88/dbmeta",
    python_requires=">=3.8",
    license="MIT"
)