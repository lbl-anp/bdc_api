import setuptools

with open("README.md", "r") as f:
    long_description = f.read()

setuptools.setup(
        name="bdc_api",
        version="v1.1",
        data_files = [("", ["LICENSE.txt"])],
        install_requires=['pymongo==2.9.5', 
            'requests','six'],
        author="Hamdy Elgammal",
        author_email="hhelgammal@lbl.gov",
        long_description=long_description,
        long_description_content_type="text/markdown",
        url="",
        packages=setuptools.find_packages(),
        license="BSD-3-Clause-LBNL",
        classifiers=[
            "Programming Language :: Python :: 2",
            ],
        )
