import * as chai from 'chai'
import * as Redis from 'ioredis'
import { collect } from 'streaming-iterables'
import { Graph } from './'
const assert = chai.assert

const testRedisUrl = 'redis://localhost/2'

describe('The readme code', () => {
  beforeEach(async () => {
    const redis = new Redis(testRedisUrl)
    await redis.flushdb()
    redis.disconnect()
  })
  it('works', async () => {
    // const redisUrl = 'redis://localhost/1'
    const graph = new Graph(testRedisUrl)

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
    // for await (node of graph.allNodes()) {
    //   console.log(node)
    // }
    // { id: 1, title: 'foo' }
    // { id: 2, name: 'james' }

    // use streaming-iterables to make an array of the async iterator
    const allNodes = await collect(graph.allNodes())
    assert.deepEqual(allNodes, [{ id: 1, title: 'foo' }, { id: 2, name: 'james' }])

    graph.disconnect()
  })
})
