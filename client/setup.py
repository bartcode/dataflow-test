from setuptools import find_packages
from setuptools import setup

setup(
    name='eve_market_price',
    version='0.1',
    install_requires=[
        'google-cloud-pubsub==1.0.2',
        'protobuf==3.11.1',
    ],
    packages=find_packages(),
    include_package_data=True,
    description='Message pusher.',
)
