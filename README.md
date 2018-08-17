# Nemesis DB
[![Build Status](https://travis-ci.org/reconbot/nemesis-db.svg?branch=master)](https://travis-ci.org/reconbot/nemesis-db)
[![codecov](https://codecov.io/gh/reconbot/nemesis-db/branch/master/graph/badge.svg)](https://codecov.io/gh/reconbot/nemesis-db)


[Nemesis DB](https://www.arcade-museum.com/game_detail.php?game_id=8842) is an open source port of Bustle's Gradius graph database.

![](nemesis.jpg)


## Intro

This is a simple adjacency graph database with only a few features. It has been designed for very fast reads. While it handles writes with ease whenever a tradeoff favors reads, that tradeoff will be taken.

## Features
- [x] 32 bit GUID
- [x] Schemaless Nodes
- [x] Weighted Edges
- [x] Compression

## Future Features
- Node Schemas, types and Interfaces
- filtering edges based upon weight
- Deleting
- Edge Schemas
- Field Validations
- Edge Validations
- Edge scanning
- Node scanning
- Named Edges
- Field Indexing
- Binary packing

## Examples

```js
import { Graph } from 'nememis-db'

const graph = new Graph('redis://localhost/1')

const book = await graph.createNode({ title: 'foo', pages: 24 })
assert.deepEqual(book, { id: 1, title: 'foo', pages: 24 })

// update the data of an existing node
const updatedBook = await graph.updateNode({ id: book.id, title: 'bar' })
assert.deepEqual(updatedBook, { id: 1, title: 'bar', pages: 24 })

// replace the data of an existing node
const replacedBook = await graph.putNode({ id: book.id, title: 'foo' })
assert.deepEqual(replacedBook, { id: 1, title: 'foo' })

const author = await graph.createNode({ name: 'james' })
assert.deepEqual(author, { id: 2, name: 'james' })

// connect the book and it's author
const edge = await graph.createEdge({ subject: book.id, predicate: 'BookHasAuthor', object: author.id })
assert.deepEqual(edge, { subject: 1, predicate: 'BookHasAuthor', object: 2, weight: 0 })

const authors = await graph.findEdges({ subject: book.id, predicate: 'BookHasAuthor' })
assert.deepEqual(authors, [{ subject: 1, predicate: 'BookHasAuthor', object: 2, weight: 0 }])
const object = await graph.findNode(authors[0].object)
assert.deepEqual(object, { id: 2, name: 'james' })

// Get an async iterator of all the nodes
for await (node of graph.allNodes()) {
  console.log(node)
}
// { id: 1, title: 'foo' }
// { id: 2, name: 'james' }

// use streaming-iterables to make an array of the async iterator
const allNodes = await collect(graph.allNodes())
assert.deepEqual(allNodes, [{ id: 1, title: 'foo' }, { id: 2, name: 'james' }])
```

## API Type Defs

```ts
export declare class Graph {
    readonly config: GraphConfig;
    readonly messagePack: messagePack.MessagePack;
    readonly redis: Redis.Redis;
    constructor(redisUrl: string, config?: GraphConfigInput);
    allNodes({ batchSize }?: NodeScanOptions): AsyncIterableIterator<Node>;
    createEdge({ subject, predicate, object, weight }: EdgeInput): Promise<Edge>;
    createNode(attributes: any): Promise<Node>;
    disconnect(): void;
    findEdges(edge: SubjectEdgeSearch | ObjectEdgeSearch): Promise<ReadonlyArray<Edge>>;
    findNode(id: number): Promise<Node | null>;
    nodeExists(id: number): Promise<boolean>;
    putNode(node: Node): Promise<Node>;
    updateNode(node: Node): Promise<Node>;
    private evalCommands;
    private evalCreateEdge;
    private getNextId;
    private nodeKey;
}

export interface Node {
    readonly id: number;
    readonly [key: string]: any;
}
export interface GraphConfigInput {
    readonly compressors?: ReadonlyArray<Compressor>
    readonly edgePrefix?: string;
    readonly guidKey?: string;
    readonly nodeIndexKey?: string;
    readonly nodeKeyPrefix?: string;
}
export interface GraphConfig {
    readonly edgePrefix: string;
    readonly guidKey: string;
    readonly nodeIndexKey: string;
    readonly nodeKeyPrefix: string;
}
export interface EdgeInput {
    readonly object: number;
    readonly predicate: string;
    readonly subject: number;
    readonly weight?: number;
}
export interface Edge {
    readonly object: number;
    readonly predicate: string;
    readonly subject: number;
    readonly weight: number;
}
export interface SubjectEdgeSearch {
    readonly limit?: number;
    readonly offset?: number;
    readonly predicate: string;
    readonly subject: number;
}
export interface ObjectEdgeSearch {
    readonly limit?: number;
    readonly object: number;
    readonly offset?: number;
    readonly predicate: string;
}
export interface NodeScanOptions {
    readonly batchSize?: number;
}
export interface Compressor {
    readonly compress: (Buffer: any) => Buffer | Promise<Buffer>;
    readonly decompress: (Buffer: any) => Buffer | Promise<Buffer>;
    readonly name: string;
}
```

## Developing

Tests use your local redis db 2 by default and are destructive. To use another DB submit a patch to make this configurable.
