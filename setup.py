"""
How to sent this package onto PyPi?

1) Building Package Release Tar:
```python setup.py sdist```

2) Upload Package to PyPi:
```pip install twine```
```twine upload dist/*```
"""
import pathlib
import setuptools
from setuptools import find_packages

with open("README.md", "r") as fh:

    long_description = fh.read()

setuptools.setup(

    name="batch-framework",

    version='0.0.0',

    author="jeffreylin",

    author_email="jeffrey82221@gmail.com",

    description="Lightweight ETL Framework",

    long_description=long_description,

    long_description_content_type="text/markdown",

    packages=find_packages(exclude=('tests', 'examples')),

    classifiers=[

        "Programming Language :: Python :: 3",

        "License :: OSI Approved :: MIT License",

        "Operating System :: OS Independent",

    ],
    python_requires='>=3.8, !=3.11.*',
    tests_require=['pytest'],
    install_requires=[
        'pandas',
        'dropbox',
        'pyarrow',
        'duckdb'
    ]
)
