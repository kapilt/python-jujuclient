from setuptools import setup

long_description = """
Python client for juju-core websocket api.
"""

setup(
    name="jujuclient",
    version="0.18.5",
    description="A juju-core/gojuju simple synchronous python api client.",
    author="Kapil Thangavelu",
    author_email="kapil.foss@gmail.com",
    url="http://juju.ubuntu.com",
    install_requires=["websocket-client>=0.18.0"],
    classifiers=[
        "Development Status :: 4 - Beta",
        "Programming Language :: Python",
        "Topic :: Internet",
        "Topic :: Software Development :: Libraries :: Python Modules",
        "Intended Audience :: Developers"],
    py_modules=["jujuclient"])
