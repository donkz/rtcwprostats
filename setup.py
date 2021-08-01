import setuptools


with open("README.md") as fp:
    long_description = fp.read()


setuptools.setup(
    name="rtcwprostats",
    version="0.0.1",

    description="RTCW Pro stats processing system",
    long_description=long_description,
    long_description_content_type="text/markdown",

    author="donkz",

    package_dir={"": "rtcwprostats"},
    packages=setuptools.find_packages(where="rtcwprostats"),

    install_requires=[
        "aws-cdk-lib==2.0.0-rc.10",
        "constructs>=10.0.0,<11.0.0",
    ],

    python_requires=">=3.6",

    classifiers=[
        "Development Status :: 1 - Initial Development",
        "Intended Audience :: Developers",
        "License :: OSI Approved :: Apache Software License",
        "Programming Language :: Python :: 3.8",
        "Topic :: Software Development :: Code Generators",
        "Topic :: Utilities",
        "Typing :: Typed",
    ],
)
