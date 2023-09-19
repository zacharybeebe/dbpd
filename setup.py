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

with open('README.md', mode='r', encoding='utf-8') as f:
    long_description = f.read()

with open('requirements.txt', 'r') as f:
    install_requires = [i.replace('\n', '') for i in f.readlines()]

setuptools.setup(
    name='dbpd',
    version='1.1',
    author='Zach Beebe',
    author_email='z.beebe@yahoo.com',
    description='Python module for establishing a working relationship between relational databases and Pandas DataFrames',
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
    install_requires=install_requires,
    python_requires='>=3.8',
    py_modules=['dbpd'],
    include_package_data=True
)
