from pathlib import Path

from setuptools import find_namespace_packages, setup


ROOT = Path(__file__).resolve().parent
README = ROOT / "README.md"


setup(
    name="hypertrader",
    version="0.1.2",
    description="Async Hyperliquid trading helper built on the async SDK.",
    long_description=README.read_text(encoding="utf-8"),
    long_description_content_type="text/markdown",
    author="darkerego",
    py_modules=["hypertrader"],
    repository = "https://github.com/darkerego/hypertrader",
    homepage = "https://github.com/darkerego/hypertrader",
    license = "MIT",
    packages=find_namespace_packages(include=["modes", "modes.*", "utils", "utils.*"]),
    include_package_data=True,
    python_requires=">=3.10",
    install_requires=[
        "eth-account",
        "hyperliquid-python-sdk-async",
        "python-dotenv",
        "uvloop",
        "colored",
    ],
    extras_require={
        "auto": [
            "numpy",
            "TA-Lib",
        ],
    },
    entry_points={
        "console_scripts": [
            "hypertrader=hypertrader:main",
            "ht=hypertrader:main",
        ],
    },
    classifiers=[
        "Programming Language :: Python :: 3",
        "Programming Language :: Python :: 3.10",
        "Programming Language :: Python :: 3.11",
        "Programming Language :: Python :: 3.12",
        "Operating System :: OS Independent",
    ],
)
