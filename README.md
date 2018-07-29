# Nemesis DB
[![Build Status](https://travis-ci.org/reconbot/nemesis-db.svg?branch=master)](https://travis-ci.org/reconbot/nemesis-db)
[![codecov](https://codecov.io/gh/reconbot/nemesis-db/branch/master/graph/badge.svg)](https://codecov.io/gh/reconbot/nemesis-db)


[Nemesis DB](https://www.arcade-museum.com/game_detail.php?game_id=8842) is an open source port of Bustle's Gradius graph database.

![](nemesis.jpg)


## Intro

This is a simple adjacency graph database with only a few features. It has been designed for very fast reads. While it handles writes with ease whenever a tradeoff favors reads, that tradeoff will be taken.

## Features
- [x] 32 bit GUID
- [x] Schemaless Nodes with Types
- [x] Weighted Edges

## Future Features
- filtering edges based upon weight
- Deleting
- Node Schemas and Interfaces
- Edge Schemas
- Field Validations
- Edge Validations
- Compression
- Edge scanning
- Node scanning
- Named Edges
- Field Indexing
- Binary packing

## API

```js
import { Graph } from 'nememis-db'

const graph = new Graph('redis://localhost/1')

const book = await graph.createNode({ title: 'foo' })
// { id: 1, title: 'foo' }
await graph.updateNode({ ...book, title: 'bar' })
// { id: 1, title: 'bar' }
const author = await graph.createNode({ name: 'james' })
// { id: 2, name: 'james' }
await graph.createEdge({ subject: book.id, predicate: 'BookHasAuthor', object: author.id })
// { subject: 1, predicate: 'BookHasAuthor', object: 2, weight: 0 }
const authors = await graph.findEdges({ subject: book.id, predicate: 'BookHasAuthor' })
// [{ subject: 1, predicate: 'BookHasAuthor', object: 2, weight: 0 }]
await graph.findNode(authors[0].object)
// { id: 2, name: 'james' }
```


## Developing

Tests use your local redis db 2 by default and are destructive. To use another DB submit a patch to make this configurable.
