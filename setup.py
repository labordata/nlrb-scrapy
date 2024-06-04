from setuptools import find_packages, setup

setup(
    name="nlrb",
    version="1.0",
    packages=find_packages(),
    install_requires=[
        "scrapy",
    ],
    entry_points={
        "scrapy": [
            "settings = nlrb.settings",
        ],
    },
)
