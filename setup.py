import setuptools

keywords = [
    'database',
    'pandas',
    'DataFrame'
    'Access',
    'MySQL',
    'Oracle',
    'Postgres',
    'SQLite'
]

with open('README.md', mode='r', encoding='utf-8') as fh:
    long_description = fh.read()

setuptools.setup(
    name='dbpd',
    version='1.0',
    author='Zach Beebe',
    author_email='z.beebe@yahoo.com',
    description='Python module connecting to various database types, returns user-defined sql queries as pandas DataFrames',
    long_description=long_description,
    long_description_content_type='text/markdown',
    url='https://github.com/zacharybeebe/dbpd',
    license='MIT',
    packages=setuptools.find_packages(),
    classifiers=[
        "Development Status :: 5 - Production/Stable",
        "Intended Audience :: Science/Research",
        "Programming Language :: Python :: 3",
        "License :: OSI Approved :: MIT License",
        "Operating System :: OS Independent"],
    keywords=keywords,
    python_requires='>=3.8',
    py_modules=['dbpd'],
    include_package_data=True
)
