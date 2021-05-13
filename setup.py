import os
from setuptools import setup, find_packages

with open('README.md', 'r') as fh:
    long_description = fh.read()

setup(
    name='syngenta_digital_dta',
    version=os.getenv('CIRCLE_TAG'),
    url='https://github.com/syngenta-digital/dta-python.git',
    author='Paul Cruse III, Technical Lead, Syngenta Digital',
    author_email='paul.cruse@syngenta.com',
    description='A DRY multi-database normalizer.',
    long_description=long_description,
    long_description_content_type='text/markdown',
    packages=find_packages(),
    python_requires='>=3.0',
    install_requires=[
        'aws-psycopg2==1.2.1',
        'boto3==1.17.26',
        'elasticsearch==7.12.0',
        'jsonref==0.2',
        'jsonpickle==2.0.0',
        'pyyaml==5.4.1',
        'requests-aws4auth==1.0.1',
        'simplejson==3.17.2'
    ],
    classifiers=[
        'Environment :: Web Environment',
        'Intended Audience :: Developers',
        'Topic :: Internet :: WWW/HTTP :: Dynamic Content',
        'Topic :: Software Development :: Libraries :: Python Modules',
        'Programming Language :: Python',
        'Programming Language :: Python :: 3',
        'Programming Language :: Python :: 3.6',
        'Programming Language :: Python :: 3.7'
    ]
)
