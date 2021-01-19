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
        "aws-cdk.core>=1.74.0",
        "aws-cdk.aws-s3>=1.74.0",
        "aws-cdk.aws-iam>=1.74.0",
        "aws-cdk.aws-lambda>=1.74.0",
        "aws-cdk.aws-sns>=1.74.0",
        "aws-cdk.aws-s3-notifications>=1.74.0",
        "aws-cdk.aws-sns-subscriptions>=1.74.0",
        "aws-cdk.aws-apigateway>=1.74.0",
        "aws-cdk.aws-route53>=1.74.0",
        "aws-cdk.aws_route53_targets",
        "aws-cdk.aws_events_targets",
        "aws-cdk.aws_dynamodb",
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
