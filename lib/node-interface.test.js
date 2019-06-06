import { join } from 'bluebird'
import { collect as collectStream } from 'bluestream'
import { collect as collectItr } from 'streaming-iterables'
import { testConnectGraph } from './test/graph'

describe('GraphNodeInterfaceLoader', () => {
  let graph, redis

  before(() => {
    graph = testConnectGraph()
    redis = graph._redis
  })
  beforeEach(async () => {
    await redis.flushdb()
    graph.resetStats()
  })

  describe('find methods', () => {
    const context = {}
    beforeEach(async () => {
      context.car = await graph.Car.create({ make: 'foobar', model: 'Vehicle Car' })
    })
    describe('#find', () => {
      it('finds a NodeType through a NodeInterfaceType', async () => {
        const { car } = context
        const vehicle = await graph.Vehicle.find(car.id)
        assert.equal(vehicle.id, car.id)
        assert.equal(vehicle._nodeType, car._nodeType)
      })

      it('returns null for a non existent id', async () => {
        assert.isNull(await graph.Vehicle.find(99999999))
      })

      it('rejects if the node does not implement the interface', async () => {
        const passenger = await graph.Passenger.create({ name: 'Tyler' })
        assert.isNull(await graph.Vehicle.find(passenger.id))
      })
    })

    describe('#FIND', () => {
      it('finds a node by id scoped for type', async () => {
        const { id } = await graph.Passenger.create({ name: 'Will' })
        await assert.isRejected(graph.Car.FIND(id), /Car:\d+ does not exist/)
      })
    })

    describe('#findNode', () => {
      it('finds a node by id scoped for type', async () => {
        const { car } = context
        const vehicle = await graph.Vehicle.find(car.id)
        assert.equal(vehicle.id, car.id)
        assert.equal(vehicle._nodeType, car._nodeType)
      })
    })

    describe('#findBy', () => {
      it('findBy returns all fields', async () => {
        const model = 'foo'
        const { id } = await graph.Car.create({ make: 'foobar', model })
        const car = await graph.Vehicle.findBy('model', model)
        assert.containSubset(car, {
          id,
          _nodeType: 'Car',
          model,
        })
        assert.property(car, 'updatedAt')
        assert.property(car, 'createdAt')
      })

      it('returns null when nothing exists', async () => {
        const node = await graph.Vehicle.findBy('model', 'NAN_GRAM')
        assert.isNull(node)
      })
    })

    describe('#FINDBY', () => {
      it('findBy returns all fields', async () => {
        const model = 'foo'
        const { id } = await graph.Car.create({ make: 'foobar', model })
        const car = await graph.Vehicle.FINDBY('model', model)
        assert.containSubset(car, {
          id,
          _nodeType: 'Car',
          model,
        })
        assert.property(car, 'updatedAt')
        assert.property(car, 'createdAt')
      })

      it('returns null when nothing exists', async () => {
        await assert.isRejected(graph.Vehicle.FINDBY('model', 'NAN_GRAM'), 'Vehicle.model:"NAN_GRAM" does not exist')
      })
    })

    describe('#findByRange', () => {
      it('returns an empty array when looking up by range', async () => {
        await graph.Car.create({ make: 'ford', model: 'gocar', nextMaintenance: Date.now() - 1000 })
        const nodes = await graph.Vehicle.findByRange('nextMaintenance', Date.now(), Date.now() + 10000)
        assert.lengthOf(nodes, 0)
      })

      it('returns an array when looking up by range', async () => {
        await graph.Car.create({ make: 'ford', model: 'gocar', nextMaintenance: Date.now() - 1000 })
        await graph.Van.create({ make: 'ford', model: 'gocar', marked: true, nextMaintenance: Date.now() - 1000 })
        const nodes = await graph.Vehicle.findByRange('nextMaintenance', 0, Date.now())
        assert.lengthOf(nodes, 2)
      })
    })
  })

  describe('#paginate', () => {
    const context = {}
    beforeEach(async () => {
      const vehicles = await Promise.all([
        graph.Car.create({ make: 'ford', model: 'gocar', nextMaintenance: 1 }),
        graph.Car.create({ make: 'ford', model: 'gocar', nextMaintenance: 5 }),
        graph.Car.create({ make: 'ford', model: 'gocar', nextMaintenance: 3 }),
        graph.Van.create({ make: 'ford', model: 'gocar', marked: false, nextMaintenance: 2 }),
        graph.Van.create({ make: 'ford', model: 'gocar', marked: false, nextMaintenance: 4 }),
      ])

      context.vehicles = vehicles
      graph.resetStats()
    })

    it('returns all ascending results', async () => {
      let cursor
      for (let i = 0; i < 5; i++) {
        const vehicle = context.vehicles[i]
        const {
          cursor: nextCursor,
          nodes: [node],
        } = await graph.Vehicle.paginate({
          fields: true,
          field: 'id',
          count: 1,
          cursor,
        })
        cursor = nextCursor
        assert.isOk(cursor)
        assert.containSubset(node, { id: vehicle.id, _nodeType: vehicle._nodeType })
      }
      const { cursor: nextCursor, nodes } = await graph.Vehicle.paginate({
        field: 'nextMaintenance',
        count: 1,
        cursor,
      })
      assert.equal(nextCursor, null)
      assert.deepEqual(nodes, [])
    })

    it('returns all results and nodes returned if possible', async () => {
      let cursor
      for (let i = 0; i < 5; i++) {
        const vehicle1 = context.vehicles[i]
        i++
        const vehicle2 = context.vehicles[i]
        const {
          cursor: nextCursor,
          nodes: [node1, node2],
        } = await graph.Vehicle.paginate({
          field: 'id',
          count: 2,
          cursor,
        })
        cursor = nextCursor
        assert.isOk(cursor)
        assert.containSubset(node1, { id: vehicle1.id, _nodeType: vehicle1._nodeType })
        if (vehicle2) {
          assert.containSubset(node2, { id: vehicle2.id, _nodeType: vehicle2._nodeType })
        }
      }
      const { cursor: nextCursor, nodes } = await graph.Vehicle.paginate({
        field: 'nextMaintenance',
        count: 2,
        cursor,
      })
      assert.equal(nextCursor, null)
      assert.deepEqual(nodes, [])
    })

    it('returns all descending results', async () => {
      let cursor
      let { vehicles } = context
      // orders are reversed per type
      vehicles = [vehicles[2], vehicles[1], vehicles[0], vehicles[4], vehicles[3]]
      for (let i = 0; i < 5; i++) {
        const vehicle = vehicles[i]
        const {
          cursor: nextCursor,
          nodes: [node],
        } = await graph.Vehicle.paginate({
          order: 'DESC',
          field: 'id',
          count: 1,
          cursor,
        })
        cursor = nextCursor
        assert.isOk(cursor)
        assert.containSubset(node, { id: vehicle.id, _nodeType: vehicle._nodeType })
      }
      const { cursor: nextCursor, nodes } = await graph.Vehicle.paginate({
        field: 'nextMaintenance',
        order: 'DESC',
        count: 1,
        cursor,
      })
      assert.equal(nextCursor, null)
      assert.deepEqual(nodes, [])
    })
  })

  describe('#update', () => {
    it('updates a typed node', async () => {
      const { id } = await graph.Car.create({ make: 'foo', model: 'new!' })
      const node = await graph.Vehicle.update({ id, model: 'baz' })
      assert.containSubset(node, { id, model: 'baz' })
    })

    it('rejects if you try and update a node without supplying an id', async () => {
      await assert.isRejected(graph.Vehicle.update({ model: 'baz' }), /"update.id" is required/)
    })

    it('rejects if you try and update a node that does not exist', async () => {
      await assert.isRejected(graph.Vehicle.update({ id: 1000, model: 'baz' }), /Vehicle:\d+ does not exist/)
    })

    it("rejects if you try and update a node that doesn't implement the interface", async () => {
      const { id } = await graph.Passenger.create({ name: 'Jada' })
      await assert.isRejected(graph.Vehicle.update({ id, model: 'baz' }), /Vehicle:\d+ does not exist/)
    })

    it('rejects if you try to change the node type', async () => {
      const { id } = await graph.Car.create({ make: 'GraphCar', model: 'GO!' })
      await assert.isRejected(
        graph.Vehicle.update({ id, model: 'baz', _nodeType: 'Van' }),
        /Cannot change Car:\d+ to a Van/
      )
    })
  })

  describe('#delete', async () => {
    const context = {}
    beforeEach(async () => {
      const nextMaintenance = Date.now()
      context.car = await graph.Car.create({
        make: 'foocar',
        model: 'Vehicle Car',
        nextMaintenance,
      })
      context.nextMaintenance = nextMaintenance
    })
    it('deletes the node', async () => {
      const { car } = context
      await graph.Vehicle.delete(car.id)
      assert.isNull(await graph.Vehicle.find(car.id))
    })

    it("deletes the node's id from the interface's indexes", async () => {
      const { car, nextMaintenance } = context
      const lowerInterfaceName = graph.Vehicle.name.toLowerCase()
      const preDeleteCarIdScore = await redis.zscore(`${lowerInterfaceName}:index:id`, car.id)
      const preDeleteCarNMScore = await redis.zscore(`${lowerInterfaceName}:index:nextMaintenance`, car.id)
      assert.equal(preDeleteCarIdScore, 0)
      assert.equal(preDeleteCarNMScore, nextMaintenance)

      await graph.Vehicle.delete(car.id)

      const postDeleteCarIdScore = await redis.zscore(`${lowerInterfaceName}:index:id`, car.id)
      const postDeleteCarNMScore = await redis.zscore(`${lowerInterfaceName}:index:nextMaintenance`, car.id)
      assert.equal(postDeleteCarIdScore, null)
      assert.equal(postDeleteCarNMScore, null)
    })
  })

  describe('#count', () => {
    beforeEach(async () => {
      await Promise.all([
        graph.Car.create({ make: 'foobar', model: 'Vehicle Car', nextMaintenance: 1 }),
        graph.Van.create({ model: 'Vehicle Van', make: 'foo', marked: true, nextMaintenance: 2 }),
        graph.Van.create({ model: 'Vehicle Van', make: 'foo', marked: false }),
      ])
    })

    it('counts all cars', async () => {
      const count = await graph.Vehicle.count()
      assert.equal(count, 3)
    })
  })

  describe('#scan', () => {
    beforeEach(async () => {
      await join(
        graph.Car.create({ make: 'foo', model: 'Vehicle Car', nextMaintenance: 1 }),
        graph.Van.create({ make: 'foo', model: 'Vehicle Van', marked: false, nextMaintenance: 2 }),
        graph.Van.create({ make: 'foo', model: 'Vehicle Van', marked: false, nextMaintenance: 3 })
      )
      graph.resetStats()
    })

    it('returns all results', async () => {
      const nodes = await collectStream(graph.Vehicle.scan())
      assert(nodes.length, 3)
      const nodeTypes = nodes.map(({ _nodeType }) => _nodeType).sort()
      assert.deepEqual(nodeTypes, ['Car', 'Van', 'Van'])
    })

    it('returns all results and fields', async () => {
      const stream = graph.Vehicle.scan({ fields: true })
      const nodes = await collectStream(stream)
      assert(nodes.length, 4)
      nodes.sort((a, b) => a._nodeType.localeCompare(b._nodeType))
      assert.containSubset(nodes, [
        { _nodeType: 'Car', model: 'Vehicle Car' },
        { _nodeType: 'Van', model: 'Vehicle Van' },
        { _nodeType: 'Van', model: 'Vehicle Van' },
      ])
    })

    describe('non id indexed fields', () => {
      it('returns all results', async () => {
        const nodes = await collectStream(graph.Vehicle.scan({ indexedField: 'nextMaintenance' }))
        assert(nodes.length, 3)
        const nodeTypes = nodes.map(({ _nodeType }) => _nodeType).sort()
        assert.deepEqual(nodeTypes, ['Car', 'Van', 'Van'])
      })

      it('returns all results and fields', async () => {
        const stream = graph.Vehicle.scan({ fields: true, indexedField: 'nextMaintenance' })
        const nodes = await collectStream(stream)
        assert(nodes.length, 4)
        nodes.sort((a, b) => a._nodeType.localeCompare(b._nodeType))
        assert.containSubset(nodes, [
          { _nodeType: 'Car', model: 'Vehicle Car' },
          { _nodeType: 'Van', model: 'Vehicle Van' },
          { _nodeType: 'Van', model: 'Vehicle Van' },
        ])
      })

      it('cannot scan a non numeric indexed field', async () => {
        assert.throw(() => graph.Vehicle.scan({ fields: true, indexedField: 'make' }))
      })
    })
  })

  describe('#batchScan', () => {
    beforeEach(async () => {
      await Promise.all([
        graph.Car.create({ make: 'foobar', model: 'Vehicle Car', nextMaintenance: 1 }),
        graph.Car.create({ make: 'foobar', model: 'Vehicle Car', nextMaintenance: 2 }),
        graph.Van.create({ make: 'foobar', model: 'Vehicle Van', marked: true, nextMaintenance: 3 }),
        graph.Van.create({ make: 'foobar', model: 'Vehicle Van', marked: true, nextMaintenance: 4 }),
      ])
      graph.resetStats()
    })

    it('returns all results in batches', async () => {
      const stream = graph.Vehicle.batchScan({ batchSize: 4 })
      const batches = await collectStream(stream)
      assert(batches.length, 2)
      batches.forEach(nodes => {
        assert.equal(nodes.length, 2)
      })
    })

    it('returns all results and fields in batches', async () => {
      const stream = graph.Vehicle.batchScan({ fields: true })
      const batches = await collectStream(stream)
      batches[0].forEach(data => {
        assert.containSubset(data, { _nodeType: 'Car' })
      })
    })

    it('deals with streams ending early', async () => {
      const asyncWork = []
      for (let index = 0; index < 500; index++) {
        asyncWork.push(graph.Car.create({ make: 'GraphCar', model: 'GO!' }))
      }
      await Promise.all(asyncWork)
      const stream = graph.Vehicle.batchScan({ batchSize: 10 })
      const batches = await collectStream(stream)
      const items = []
      batches.forEach(batch => items.push(...batch))
      assert.equal(items.length, 504)
    })

    describe('non id indexed fields', () => {
      it('returns all results in batches', async () => {
        const stream = graph.Vehicle.batchScan({ batchSize: 4, indexedField: 'nextMaintenance' })
        const batches = await collectStream(stream)
        assert(batches.length, 2)
        batches.forEach(nodes => {
          assert.equal(nodes.length, 2)
        })
      })

      it('returns all results and fields in batches', async () => {
        const stream = graph.Vehicle.batchScan({ fields: true, indexedField: 'nextMaintenance' })
        const batches = await collectStream(stream)
        batches[0].forEach(data => {
          assert.containSubset(data, { _nodeType: 'Car' })
        })
      })

      it('deals with streams ending early', async () => {
        const asyncWork = []
        for (let index = 0; index < 500; index++) {
          asyncWork.push(graph.Car.create({ make: 'GraphCar', model: 'GO!', nextMaintenance: index + 100 }))
        }
        await Promise.all(asyncWork)
        const stream = graph.Vehicle.batchScan({ batchSize: 10 })
        const batches = await collectStream(stream)
        const items = []
        batches.forEach(batch => items.push(...batch))
        assert.equal(items.length, 504)
      })

      it('cannot batch scan a non numeric indexed field', async () => {
        assert.throw(() => graph.Vehicle.batchScan({ fields: true, indexedField: 'make' }))
      })
    })
  })

  describe('#scanItr', () => {
    beforeEach(async () => {
      await join(
        graph.Car.create({ make: 'foo', model: 'Vehicle Car', nextMaintenance: 1 }),
        graph.Van.create({ make: 'foo', model: 'Vehicle Van', marked: false, nextMaintenance: 2 }),
        graph.Van.create({ make: 'foo', model: 'Vehicle Van', marked: false, nextMaintenance: 3 })
      )
      graph.resetStats()
    })

    it('returns all results', async () => {
      const nodes = await collectItr(graph.Vehicle.scanItr())
      assert(nodes.length, 3)
      const nodeTypes = nodes.map(({ _nodeType }) => _nodeType).sort()
      assert.deepEqual(nodeTypes, ['Car', 'Van', 'Van'])
    })

    it('returns all results and fields', async () => {
      const stream = graph.Vehicle.scanItr({ fields: true })
      const nodes = await collectItr(stream)
      assert(nodes.length, 4)
      nodes.sort((a, b) => a._nodeType.localeCompare(b._nodeType))
      assert.containSubset(nodes, [
        { _nodeType: 'Car', model: 'Vehicle Car' },
        { _nodeType: 'Van', model: 'Vehicle Van' },
        { _nodeType: 'Van', model: 'Vehicle Van' },
      ])
    })

    describe('non id indexed fields', () => {
      it('returns all results', async () => {
        const nodes = await collectItr(graph.Vehicle.scanItr({ indexedField: 'nextMaintenance' }))
        assert(nodes.length, 3)
        const nodeTypes = nodes.map(({ _nodeType }) => _nodeType).sort()
        assert.deepEqual(nodeTypes, ['Car', 'Van', 'Van'])
      })

      it('returns all results and fields', async () => {
        const stream = graph.Vehicle.scanItr({ fields: true, indexedField: 'nextMaintenance' })
        const nodes = await collectItr(stream)
        assert(nodes.length, 4)
        nodes.sort((a, b) => a._nodeType.localeCompare(b._nodeType))
        assert.containSubset(nodes, [
          { _nodeType: 'Car', model: 'Vehicle Car' },
          { _nodeType: 'Van', model: 'Vehicle Van' },
          { _nodeType: 'Van', model: 'Vehicle Van' },
        ])
      })

      it('cannot scan a non numeric indexed field', async () => {
        await assert.isRejected(graph.Vehicle.scanItr({ fields: true, indexedField: 'make' }).next())
      })
    })
  })

  describe('#batchScanItr', () => {
    beforeEach(async () => {
      await Promise.all([
        graph.Car.create({ make: 'foobar', model: 'Vehicle Car', nextMaintenance: 1 }),
        graph.Car.create({ make: 'foobar', model: 'Vehicle Car', nextMaintenance: 2 }),
        graph.Van.create({ make: 'foobar', model: 'Vehicle Van', marked: true, nextMaintenance: 3 }),
        graph.Van.create({ make: 'foobar', model: 'Vehicle Van', marked: true, nextMaintenance: 4 }),
      ])
      graph.resetStats()
    })

    it('returns all results in batches', async () => {
      const stream = graph.Vehicle.batchScanItr({ batchSize: 4 })
      const batches = await collectItr(stream)
      assert(batches.length, 2)
      batches.forEach(nodes => {
        assert.equal(nodes.length, 2)
      })
    })

    it('returns all results and fields in batches', async () => {
      const stream = graph.Vehicle.batchScanItr({ fields: true })
      const batches = await collectItr(stream)
      batches[0].forEach(data => {
        assert.containSubset(data, { _nodeType: 'Car' })
      })
    })

    it('deals with streams ending early', async () => {
      const asyncWork = []
      for (let index = 0; index < 500; index++) {
        asyncWork.push(graph.Car.create({ make: 'GraphCar', model: 'GO!' }))
      }
      await Promise.all(asyncWork)
      const stream = graph.Vehicle.batchScanItr({ batchSize: 10 })
      const batches = await collectItr(stream)
      const items = []
      batches.forEach(batch => items.push(...batch))
      assert.equal(items.length, 504)
    })

    describe('non id indexed fields', () => {
      it('returns all results in batches', async () => {
        const stream = graph.Vehicle.batchScanItr({ batchSize: 4, indexedField: 'nextMaintenance' })
        const batches = await collectItr(stream)
        assert(batches.length, 2)
        batches.forEach(nodes => {
          assert.equal(nodes.length, 2)
        })
      })

      it('returns all results and fields in batches', async () => {
        const stream = graph.Vehicle.batchScanItr({ fields: true, indexedField: 'nextMaintenance' })
        const batches = await collectItr(stream)
        batches[0].forEach(data => {
          assert.containSubset(data, { _nodeType: 'Car' })
        })
      })

      it('deals with streams ending early', async () => {
        const asyncWork = []
        for (let index = 0; index < 500; index++) {
          asyncWork.push(graph.Car.create({ make: 'GraphCar', model: 'GO!', nextMaintenance: index + 100 }))
        }
        await Promise.all(asyncWork)
        const stream = graph.Vehicle.batchScanItr({ batchSize: 10 })
        const batches = await collectItr(stream)
        const items = []
        batches.forEach(batch => items.push(...batch))
        assert.equal(items.length, 504)
      })

      it('cannot batch scan a non numeric indexed field', async () => {
        await assert.isRejected(graph.Vehicle.batchScanItr({ fields: true, indexedField: 'make' }).next())
      })
    })
  })

  describe('#hasInstanceOf', () => {
    it('returns true if the node implements the interface', async () => {
      const car = await graph.Car.create({ make: 'GraphCar', model: 'GO!' })
      const passenger = await graph.Passenger.create({ name: 'Francis' })
      assert.isTrue(graph.Vehicle.hasInstanceOf(car))
      assert.isFalse(graph.Vehicle.hasInstanceOf(passenger))
    })
  })
})
