avro-json-hadoop
================
Interact with Avro files in Hadoop as if they were JSON. This library primarily exists to let you use jq with Hadoop MapReduce as part of [hjq](https://github.com/bskaggs/hjq).

Want to use Avro files in Hadoop Streaming?  This package has a `AvroAsJsonKeyInputFormat` that creates (Text, NullWritable) pairs.

Other useful classes:
* `JsonAsAvroOutputFormat` will write JSON-formatted string keys as Avro files.
* `AvroOrJsonKeyInputFormat` acts like `AvroAsJsonKeyInputFormat`, but also reads in JSON files mixed in.
