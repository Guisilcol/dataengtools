from setuptools import setup, find_packages

setup(
    name='dataengtools',
    version='0.1.0',
    packages=find_packages(),
    install_requires=[
        "boto3==1.35.76",
        "boto3-stubs==1.35.90",
        "mypy-boto3==1.35.70",
        "mypy-boto3-glue==1.35.87",
        "mypy-boto3-s3==1.35.81",
        "polars==1.16.0",
        "s3fs==2024.12.0",
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