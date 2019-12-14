# Dataflow example
Example project which combines messages from Pub/Sub and data from BigQuery. There is one
message queue: `sensor`. This is a stream of numbers which - for whatever reason -
needs to be summed over a window of 5 seconds. The sum is checked against a lower and upper bound as
described in a BigQuery table (see [`./data/numbers.csv`](./data/numbers.csv)). The output will be a
record in a BQ table, which looks as follows:

| `update_time` | `number_sum` | `number_count` | `number_type` | `version` | 
| ---- | ---- | ---- | ---- | ---- |
| 1234 | 49 | 5 | tens | run-1 |
| 1235 | 490 | 100 | hundreds | run-1 |

The basic flow schema is as follows:

```text
+------------------------+                +------------------------+                +------------------------+
|                        |                |                        |                |                        |
| Google Cloud Pub/Sub   +--------------->+ Google Cloud Dataflow  +--------------->+ Google Cloud BigQuery  |
|                        |       |        |                        |                |                        |
+------------------------+       |        +------------------------+                +------------------------+
                                 |
+------------------------+       |
|                        |       |
| Google Cloud BigQuery  +-------+
|                        |
+------------------------+

```

## Set up
A few things need to be set up on the Google Cloud Platform:
- One topic in Cloud Pub/Sub:
    - `sensor`
- One subscription in Cloud Pub/Sub:
    - `sensor`
- Import [`./data/numbers.csv`](./data/numbers.csv) into a BigQuery table called `numbers`.
- Update [`application.conf`](./src/main/resources/application.conf) with your configuration.


## Running the code
The pipeline is deployed by running:

```bash
$ sbt "runMain example.SourceMerge"
```

Messages can be sent with the `client`, which is also included in this repository. See its [README.md](./client).
