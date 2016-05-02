from setuptools import setup

setup(
    name = "s3-logparser",
    version = "0.0.1",
    author = "Matthew Reid",
    author_email = "matt@nomadic-recording.com",
    description = "Collects, parses and stores Amazon S3 access logs",
    url='https://github.com/nocarryr/s3-logparser',
    license='GPLv3',
    keywords = "aws",
    packages=['s3logparse'],
    include_package_data=True,
    entry_points={
        'console_scripts':[
            's3.logparse = s3logparse.main:main',
        ],
    },
    install_requires=[
        'pytz',
        'pymongo',
        'pyaml',
    ],
    setup_requires=['setuptools-markdown'],
    long_description_markdown_filename='README.md',
    classifiers = [
        'Development Status :: 3 - Alpha',
        'Environment :: Console',
        'Intended Audience :: Developers',
        'Intended Audience :: Information Technology',
        'License :: OSI Approved :: GNU General Public License v3 (GPLv3)',
        'Operating System :: OS Independent',
        'Programming Language :: Python',
        'Topic :: Utilities',
        'Programming Language :: Python :: 2',
        'Programming Language :: Python :: 2.7',
        'Programming Language :: Python :: 3',
        'Programming Language :: Python :: 3.4',
        'Programming Language :: Python :: 3.5',
    ],
)
