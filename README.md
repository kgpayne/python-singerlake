# python-singerlake

A library for interacting with a Singerlake; a store of files in Singer JSONL format.

**Note:** This software is in active development. Here be dragons 🐉. Use with caution.

## Overview

For expended details of the Singerlake Spec, checkout [this blog post](https://kenpayne.co.uk/blog/2023-01-17.html).

Broadly the Singerlake Spec describes a way to write raw Singer messages into object stores (such as S3) in a structured way.
In addition to offering cheap long-term storage of captured data, it also unlocks several interesting patterns.
The overall aim is to roll `python-singerlake` into a `tap-singerlake` and `target-singerlake` for use in data pipelines.
