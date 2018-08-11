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

## Examples

```js
import { Graph } from 'nememis-db'

const graph = new Graph('redis://localhost/1')

const book = await graph.createNode({ title: 'foo', pages: 24 })
// { id: 1, title: 'foo' }
// patch the data of an existing node
await graph.updateNode({ id: book.id, title: 'bar' })
// { id: 1, title: 'bar', pages: 24 }
const author = await graph.createNode({ name: 'james' })
// { id: 2, name: 'james' }
await graph.createEdge({ subject: book.id, predicate: 'BookHasAuthor', object: author.id })
// { subject: 1, predicate: 'BookHasAuthor', object: 2, weight: 0 }
const authors = await graph.findEdges({ subject: book.id, predicate: 'BookHasAuthor' })
// [{ subject: 1, predicate: 'BookHasAuthor', object: 2, weight: 0 }]
await graph.findNode(authors[0].object)
// { id: 2, name: 'james' }

// Get an async iterator of all the nodes
for await (node of graph.allNodes()) {
  console.log(node)
}
// { id: 1, title: 'bar', pages: 24 }
// { id: 2, name: 'james' }

// use streaming iterables to make an array of them
import { collect } from 'streaming-iterables'
await collect(graph.allNodes())
// [{ id: 1, title: 'bar', pages: 24 }, { id: 2, name: 'james' }]
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

```

## Developing

Tests use your local redis db 2 by default and are destructive. To use another DB submit a patch to make this configurable.
