from setuptools import find_packages, setup

setup(
    description="zoo-cwltool-runner",
    url="https://github.com/ZOO-Project/zoo-cwltool-runner.git",
    author="Terradue",
    author_email="fabrice.brito@terradue.com",
    license="EUPL",
    include_package_data=True,
    packages=find_packages(),
    zip_safe=False,
    entry_points={},
    package_data={"": []},
)
