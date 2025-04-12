from setuptools import find_packages, setup

setup(
    name="dagsterPipeline",
    packages=find_packages(exclude=["dagsterPipeline_tests"]),
    install_requires=[
        "dagster",
        "dagster-cloud"
    ],
    extras_require={"dev": ["dagster-webserver", "pytest"]},
)
