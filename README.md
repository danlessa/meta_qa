# meta-qa

An practical one-liner metalanguage.

## Introduction

### What is this about

Several cloud databases providers, like Google BigQuery and Amazon Redshift,
allows to put text descriptions and labels into the schemas, and in an
column-wise manner.

Having that in mind, meta_qa is an attempt to embrace this for using the
description fields as an metadata container, especifically in an QA context.

This is implemented through an practical, flexible and lightweight syntax for
using on the description fields both on the tables and columns, and which
aims to input our knowlodge about what is contained inside them.

meta_qa, then, is an library that perform queries for getting the metadata,
parses it, generates an documentation based on it, and performs associated
operations.

### Typical use cases

* You need to document your BigQuery / Redshift datasets but you are feeling
  lazy.
* You need to perform and collect metrics from QA over and over.
* You think that it would be nice to present schema data together with what is
  expected from QA.
* You need an practical and flexible way to input metadata onto your columns.

## How to use it

First, go to your dataset and input some descriptions in your tables and columns
using the meta_qa syntax.

### meta_qa syntax

I think it sort of self-descriptive:

``Description about the table/column;@param_1=json_object;
@param_2=json_object``

An concrete example would be that:

``Timestamp for when the attempt was submitted;@title='Attempt submission datetime';
@assert=['lower_than(report_timestamp)']``

#### Reserved variables

*title (str)*: an friendly title for the column or table

*assert (list of str)*: QA functions to execute during the QA process.

*warn (list of str)*: same of above, but less strict.

*ignore (int)*: Whenever to ignore the table/column or not. 0 for False and 1 for True.

#### QA functions

*lower_than(str other_column_name)*: Asserts that every element in this column is lower than the other column in the same table.

*larger_than(str other_column_name)*:  Opposite of the above.

*not_null()*: All elements must be non-null.

*not_duplicates()*: All elements must be distinct.

*not_percentile_null(float q=0.05)*: Count of nulls must be lower than specified. Default percentile: 0.05.

*not_window_percentile_null(float days=90, float q=0.05)*: Same of above, but applied in an temporal window from now until days ago.

*timestamp_sanity()*: Filter between -1d behind a priori definded data and +1d from now.

*related_to(str other_table, str_opt other_column)*:


### Automatic documentation generation

To be written.

### Batch QA operations

To be written.

## How to modify it

To be written.
