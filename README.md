# Dataflow example
Example project which combines messages from Pub/Sub and data from BigQuery. There are two
message queues: `auto` and `manual`. Those are two streams of numbers which - for whatever reason -
need to be summed over a window of 5 seconds. The sum is checked against a lower and upper bound as
described in a BigQuery table (see [`./data/numbers.csv`](./data/numbers.csv)). The output will be a
record in a BQ table, which looks as follows:

| `timestamp` | `total` | `number_type` | 
| ---- | ---- | ---- |
| 1234 | 49 | tens |
| 1235 | 490 | hundreds |

## Set up
A few things need to be set up on the Google Cloud Platform:
- Two topics in Cloud Pub/Sub:
    - `auto`
    - `manual`
- Two subscriptions in Cloud Pub/Sub:
    - `sensor-auto`
    - `sensor-manual`
- Import [`./data/numbers.csv`](./data/numbers.csv) into a BigQuery table called `numbers`.

## Features
This project comes with number of preconfigured features, including:

### sbt-pack
Use `sbt-pack` instead of `sbt-assembly` to:
 * reduce build time
 * enable efficient dependency caching
 * reduce job submission time

To build package run:

```
sbt pack
```

### Testing
This template comes with an example of a test, to run tests:

```
sbt test
```

### Scala style
Find style configuration in `scalastyle-config.xml`. To enforce style run:

```
sbt scalastyle
```

### REPL
To experiment with current codebase in [Scio REPL](https://github.com/spotify/scio/wiki/Scio-REPL)
simply:

```
sbt repl/run
```

---

This project is based on the [scio.g8](https://github.com/spotify/scio.g8).
