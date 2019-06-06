import { testConnectGraph } from './test/graph'

describe('GraphLoader', () => {
  let graph

  before(() => {
    graph = testConnectGraph()
  })

  describe('node.find', () => {
    beforeEach(async () => {
      await graph._redis.flushdb()
    })

    it('finds a node by id', async () => {
      const { id } = await graph.Passenger.create({ name: 'Jaden' })
      graph.resetStats()
      const node = await graph.Node.FIND(id)
      assert.containSubset(node, {
        id,
        _nodeType: 'Passenger',
      })
    })

    it('typecasts arrays, booleans, dates, and finds all fields', async () => {
      const nextMaintenance = Date.now()
      const isCompact = false
      const model = 'The GMC Graphmobile'
      const { id } = await graph.Car.create({
        make: 'GMC',
        nextMaintenance,
        isCompact,
        model,
      })
      graph.resetStats()
      const node = await graph.Node.FIND(id)
      assert.containSubset(node, {
        id,
        _nodeType: 'Car',
        nextMaintenance,
        isCompact,
        model,
      })
    })

    it('serializes JSON type', async () => {
      const model = 'graph car'
      const make = 'graphGO'
      const metadata = {
        foo: ['bar', 4],
        baz: { boo: null },
      }
      const { id } = await graph.Car.create({ model, metadata, make })
      assert.containSubset(await graph.Node.FIND(id), {
        id,
        metadata,
      })
    })

    it('finds null when there is no matching id', async () => {
      assert.isNull(await graph.Node.find(10)) // not used id
    })
  })
})
