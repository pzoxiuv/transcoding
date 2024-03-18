from setuptools import setup, find_packages

setup(
    name='fslorch',
    version='0.1',
    package_dir={'': 'lib'},
    packages=find_packages(where='lib'),
)
