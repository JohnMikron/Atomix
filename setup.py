from setuptools import setup, find_packages

setup(
    name="atomix-stm",
    version="3.1.5",
    packages=find_packages(),
    install_requires=[],
    author="Atomix STM Maintainers",
    author_email="maintainers@atomix-stm.org",
    description="Production-grade Software Transactional Memory for Python 3.13+",
    long_description=open("README.md", encoding="utf-8").read(),
    long_description_content_type="text/markdown",
    url="https://github.com/JohnMikron/Atomix",
    classifiers=[
        "Programming Language :: Python :: 3",
        "Programming Language :: Python :: 3.13",
        "License :: OSI Approved :: GNU General Public License v3 (GPLv3)",
        "Operating System :: OS Independent",
    ],
    python_requires='>=3.9',
)
