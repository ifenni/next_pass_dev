from setuptools import setup

setup(
    name="next_pass",
    version="0.1.0",
    py_modules=["next_pass", "collection_builder", "landsat_pass", "sentinel_pass", "opera_products", "plot_maps", "utils"],
    install_requires=[],
    description="Tool to find upcoming satellite overpasses and OPERA product availability",
    author="OPERA-Cal-Val",
    url="https://github.com/OPERA-Cal-Val/next_pass",
    classifiers=[
        "Programming Language :: Python :: 3"
    ],
)