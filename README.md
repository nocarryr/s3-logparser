# s3-logparser

Collects, parses and stores Amazon S3 access logs (for buckets with logging enabled)

## Introduction
All accessible S3 buckets (with the given credentials) are checked for their logging configuration.  Buckets with logging enabled are inspected and, assuming the log destination bucket is accessible, the log files are read and parsed according to the format described [here](https://docs.aws.amazon.com/AmazonS3/latest/dev/LogFormat.html).

Each file/entry is then saved to a storage backend and if successfully written, the log file is deleted from the bucket (S3 logging can generate a large number of files rather quickly).
Currently, the only implemented backend is [MongoDB](https://www.mongodb.org), but others will be added.

## Requirements
* [pytz](https://pypi.python.org/pypi/pytz)
* [boto](https://pypi.python.org/pypi/boto) (Not Boto 3... yet)
* [pymongo](https://pypi.python.org/pypi/pymongo)

## TODO/Roadmap
- [ ] More storage backends
- [ ] Configuration / CLI args
- [ ] HTML Views (statically generated?)
- [ ] Packaging
- [ ] Tests, tests and maybe some tests
