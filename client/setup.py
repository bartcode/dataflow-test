from setuptools import setup, find_namespace_packages

# The version of the Kratos CLI is determined based on
# https://packaging.python.org/guides/single-sourcing-package-version/
version = {}

with open('src/pubsend/__init__.py') as fp:
    exec(fp.read(), version)

setup(
    name='pubsend',
    version=version.get('__version__', 'unknown'),
    package_dir={'': 'src'},
    packages=find_namespace_packages(where='src'),
    install_requires=[
        'google-cloud-pubsub==1.0.2',
        'protobuf==3.11.4',
        'pyhocon==0.3.51',
        'pylint==2.4.4',
        'setuptools==45.2.0',
    ],
    include_package_data=True,
    data_files=[('pubsend', ['application.conf'])],
    entry_points={
        'console_scripts': ['pubsend=pubsend.__main__:main']
    },
    description='Message client (Pub/Sub).',
)
