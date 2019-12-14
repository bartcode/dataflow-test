from setuptools import find_packages
from setuptools import setup

setup(
    name='pbsend',
    version='0.1',
    install_requires=[
        'google-cloud-pubsub==1.0.2',
        'protobuf==3.11.1',
        'pyhocon==0.3.51',
        'pylint==2.4.4',
    ],
    packages=find_packages(),
    include_package_data=True,
    data_files=[('pbsend', ['application.conf'])],
    entry_points={
        'console_scripts': ['pbsend=pbsend.__main__:main']
    },
    description='Message client.',
)
