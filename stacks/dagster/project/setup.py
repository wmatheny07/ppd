from setuptools import setup, find_packages

setup(
    name="dagster_ppd",
    packages=find_packages(exclude=["dagster_ppd_tests"]),
    install_requires=["dagster"],
)
