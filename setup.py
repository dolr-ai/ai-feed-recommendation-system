from setuptools import setup, find_packages


def read_requirements():
    try:
        with open("requirements.txt", "r") as f:
            return [line.strip() for line in f if line.strip()]
    except FileNotFoundError:
        return ["redis>=6.4.0"]


setup(
    name="recsys-on-premise",
    version="0.1.0",
    packages=find_packages(where="legacy_recommendation_system"),
    package_dir={"": "legacy_recommendation_system"},
    install_requires=read_requirements(),
)
