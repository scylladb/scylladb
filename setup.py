from setuptools import find_packages, setup

setup(
    name="scylla",
    description='NoSQL data store using the seastar framework, compatible with Apache Cassandra',
    url='https://github.com/scylladb/scylla',
    download_url='https://github.com/scylladb/scylla/tags',
    license='AGPL',
    platforms='any',
    packages=find_packages(),
    include_package_data=True,
    install_requires=[])
