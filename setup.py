from setuptools import setup

setup(
    name='geventconnectionpool',
    version='1.0.0',
    url='https://github.com/max0d41/geventconnectionpool',
    description='gevent based sqlalchemy connection pool',
    packages=[
        'geventconnectionpool',
    ],
    install_requires=[
        'gevent',
        'sqlalchemy'
    ],
    zip_safe=False
)
