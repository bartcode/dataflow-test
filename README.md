# Dataflow example
Example project which combines messages from Pub/Sub and data from BigQuery. There is one
message queue: `sensor`. This is a stream of numbers which - for whatever reason -
needs to be summed over a window of 5 seconds. The sum is checked against a lower and upper bound as
described in a BigQuery table (see [`./data/numbers.csv`](./data/numbers.csv)). The output will be a
record in a BQ table, which looks as follows:

| `timestamp` | `total` | `number_type` | 
| ---- | ---- | ---- |
| 1234 | 49 | tens |
| 1235 | 490 | hundreds |

## Set up
A few things need to be set up on the Google Cloud Platform:
- One topic in Cloud Pub/Sub:
    - `sensor`
- One subscription in Cloud Pub/Sub:
    - `sensor`
- Import [`./data/numbers.csv`](./data/numbers.csv) into a BigQuery table called `numbers`.
