from setuptools import setup, find_packages

setup(
    name='dataengtools',
    version='0.1.0',
    packages=find_packages(),
    install_requires=[
        'boto3==1.35.76',
        'mypy-boto3==1.35.70',
        'boto3-stubs[s3,glue,athena]',
        'pandas==2.2.3',
        's3fs==0.4.2'
    ],
    author='Guilherme dos Santos Magalhães',
    author_email='silcol455@gmail.com',
    description='Tools for data engineering in AWS',
    long_description_content_type='text/markdown',
    url='https://github.com/guisilcol/dataengtools',
    classifiers=[
        'Programming Language :: Python :: 3',
        'License :: OSI Approved :: MIT License',
        'Operating System :: OS Independent',
    ],
    python_requires='>=3.6',
)