#  Aggregation plugin for Elasticsearch 

This plugin adds some useful aggregations to Elasticsearch.

The `path` aggregation is copied from https://github.com/opendatasoft/elasticsearch-aggregation-pathhierarchy/

## Versions

| Elasticsearch  | Plugin      | Release date |
| -------------- | ----------- | -------------|
| 2.1.1          | 2.1.1.2     | Feb 14, 2016 |
| 2.1.1          | 2.1.1.1     | Jan 31, 2016 |
| 2.1.1          | 2.1.1.0     | Jan  5, 2016 |
| 1.5.2          | 1.5.2.0     | Sep 22, 2015 |

## Installation

### Elasticsearch 2.x

    ./bin/plugin install http://xbib.org/repository/org/xbib/elasticsearch/plugin/elasticsearch-aggregations/2.1.1.1/elasticsearch-aggregations-2.1.1.1-plugin.zip

### Elasticsearch 1.x

    ./bin/plugin -install aggregations -url http://xbib.org/repository/org/xbib/elasticsearch/plugin/elasticsearch-aggregations/1.5.2.0/elasticsearch-aggregations-1.5.2.0-plugin.zip

Do not forget to restart the node after installing.

## Project docs

The Maven project site is available at [Github](http://jprante.github.io/elasticsearch-aggregations)

## Issues

All feedback is welcome! If you find issues, please post them at [Github](https://github.com/jprante/elasticsearch-aggregations/issues)

# License

Aggregations plugin for Elasticsearch

Copyright (C) 2015 Jörg Prante

based upon https://github.com/opendatasoft/elasticsearch-aggregation-pathhierarchy/

The MIT License (MIT)

Permission is hereby granted, free of charge, to any person obtaining a copy
of this software and associated documentation files (the "Software"), to deal
in the Software without restriction, including without limitation the rights
to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
copies of the Software, and to permit persons to whom the Software is
furnished to do so, subject to the following conditions:

The above copyright notice and this permission notice shall be included in
all copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
THE SO