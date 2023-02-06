from setuptools import setup, find_packages, find_namespace_packages

with open("README.md", "r") as readme_file:
    readme = readme_file.read()

requirements = ["requests>=2", "pandas>=1", "numpy>=1", "beautifulsoup4>=1", "lxml>=1", "tqdm", "unidecode"]

setup(
    name="chickenstats",
    version="1.7.3.8.9.10.25",
    author="Chicken and Stats",
    author_email="chicken@chickenandstats.com",
    description="A library for scraping, munging, and visualizing sports data",
    long_description=readme,
    long_description_content_type="text/markdown",
    url="https://github.com/chickenandstats/chickenstats",
    packages=find_namespace_packages(include=['chickenstats.*']),
    install_requires=requirements,
    license="MIT",
    include_package_data=True,
    package_data={'': ['chicken_nhl/models/*.pickle']},
    classifiers=[
        "Programming Language :: Python :: 3.7",
        "License :: OSI Approved :: MIT License",
    ],
)