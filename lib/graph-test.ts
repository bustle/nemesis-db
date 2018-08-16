import * as chai from 'chai'
import * as chaiAsPromised from 'chai-as-promised'
import * as Redis from 'ioredis'
import { collect } from 'streaming-iterables'
import { Graph, Node } from './graph'

chai.use(chaiAsPromised)
const assert = chai.assert

const testRedisUrl = 'redis://localhost/2'

describe('Graph', () => {
  let graph: Graph
  beforeEach(async () => {
    const redis = new Redis(testRedisUrl)
    await redis.flushdb()
    redis.disconnect()
    graph = new Graph(testRedisUrl)
  })
  afterEach(() => {
    graph.disconnect()
  })

  describe('createNode', () => {
    it('creates a node', async () => {
      const bookData = {
        title: 'A brief memory of time',
        likes: 50,
        publishedAt: new Date(),
        meta: {
          foo: 'bar'
        }
      }
      const node = await graph.createNode(bookData)
      assert.deepInclude(node, bookData)
    })
  })

  describe('updateNode', () => {
    it('patches the data of an existing node', async () => {
      const bookData = {
        title: 'A brief memory of time',
        likes: 50,
      }
      const { id } = await graph.createNode(bookData)
      const updatedNode = await graph.updateNode({ id, likes: 49 }) // so sad
      assert.deepEqual(updatedNode, {
        id,
        title: 'A brief memory of time',
        likes: 49
      })
    })
    it('errors if no node exists', async () => {
      await assert.isRejected(graph.updateNode({ id: 4 }))
    })
  })

  describe('putNode', () => {
    it('replaces the whole node', async () => {
      const bookData = {
        title: 'A brief memory of time',
        likes: 50,
      }
      const { id } = await graph.createNode(bookData)
      const updatedNode = await graph.putNode({ id, title: 'lies about memory', dislikes: 1 })
      assert.deepEqual(updatedNode, {
        id,
        title: 'lies about memory',
        dislikes: 1
      })
    })
    it('errors if no node exists', async () => {
      await assert.isRejected(graph.putNode({ id: 4 }))
    })
  })

  describe('findNode', () => {
    it('returns null when no node is found', async () => {
      assert.isNull(await graph.findNode(1))
    })

    it('finds a node', async () => {
      const bookData = {
        title: 'A brief memory of time',
        likes: 50,
        publishedAt: new Date(),
        meta: {
          foo: 'bar'
        }
      }
      const node = await graph.createNode(bookData)
      assert.deepEqual(node, await graph.findNode(node.id))
    })
  })

  describe('createEdge', () => {
    it('creates an edge', async () => {
      const object = await graph.createNode({ type: 'object' })
      const subject = await graph.createNode({ type: 'subject' })
      const edge = await graph.createEdge({
        object: object.id,
        predicate: 'HasThingy',
        subject: subject.id
      })
      assert.deepEqual(edge, {
        object: object.id,
        predicate: 'HasThingy',
        subject: subject.id,
        weight: 0
      })
    })

    it('rejects if missing an object', async () => {
      const object = await graph.createNode({ type: 'object' })
      await assert.isRejected(graph.createEdge({
        object: object.id,
        predicate: 'HasThingy',
        subject: 4
      }))
    })

    it('rejects if missing a subject', async () => {
      const subject = await graph.createNode({ type: 'subject' })
      await assert.isRejected(graph.createEdge({
        object: 4,
        predicate: 'HasThingy',
        subject: subject.id
      }))
    })
  })

  describe('findEdges', () => {
    it('finds an edge with a subject and predicate', async () => {
      const object = await graph.createNode({ type: 'object' })
      const subject = await graph.createNode({ type: 'subject' })
      const edge = await graph.createEdge({
        object: object.id,
        predicate: 'HasThingy',
        subject: subject.id,
        weight: 1
      })
      await graph.createEdge({
        object: object.id,
        predicate: 'HasThingy2',
        subject: subject.id,
        weight: 2
      })
      assert.deepEqual(
        await graph.findEdges({ subject: subject.id, predicate: 'HasThingy'}),
        [edge]
      )
    })
    it('finds an edge with an object and predicate', async () => {
      const object = await graph.createNode({ type: 'object' })
      const subject = await graph.createNode({ type: 'subject' })
      await graph.createEdge({
        object: object.id,
        predicate: 'HasThingy',
        subject: subject.id,
        weight: 1
      })
      const edge2 = await graph.createEdge({
        object: object.id,
        predicate: 'HasThingy2',
        subject: subject.id,
        weight: 2
      })
      assert.deepEqual(
        await graph.findEdges({ object: object.id, predicate: 'HasThingy2'}),
        [edge2]
      )
    })
  })

  describe('allNodes', () => {
    it('returns an async iterator of all the nodes', async () => {
      const node1 = await graph.createNode({ foo: 1 })
      const node2 = await graph.createNode({ foo: 2 })
      const nodes: Node[] = []
      for await (const node of graph.allNodes()) {
        nodes.push(node)
      }
      assert.deepEqual(nodes, [node1, node2])
      assert.deepEqual(await collect(graph.allNodes()), [node1, node2])
    })
    it('paginates', async () => {
      const nodes: Array<Promise<Node>> = []
      for (let i = 0; i < 200; i++) {
        nodes.push(graph.createNode({ }))
      }
      await Promise.all(nodes)
      assert.equal((await collect(graph.allNodes({ batchSize: 1}))).length, 200)
    })
  })
})
