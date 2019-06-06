import { assert } from 'chai'
import { collect as collectStream } from 'bluestream'
import { collect as collectItr } from 'streaming-iterables'
import { testConnectGraph, HasDriver as predicate, RegistrationNumber } from './test/graph'
import { STATES, GraphDBNode } from './node-utils'

describe('GraphNodeLoader', () => {
  let graph: any
  let redis: any
  before(() => {
    graph = testConnectGraph()
    redis = graph._redis
  })
  beforeEach(async () => {
    await redis.flushdb()
    graph.resetStats()
  })

  describe('#find', () => {
    it('finds a node by id scoped for type', async () => {
      const { id } = await graph.Passenger.create({ name: 'Willow' })
      assert.isNull(await graph.Car.find(id))
    })
  })

  describe('#FIND', () => {
    it('finds a node by id scoped for type', async () => {
      const { id } = await graph.Passenger.create({ name: 'Jada' })
      await assert.isRejected(graph.Car.FIND(id), /\w+:\d+ does not exist/)
    })
  })

  describe('#findNode', () => {
    it('finds a node by id scoped for type', async () => {
      const { id } = await graph.Passenger.create({ name: 'Jaden' })
      assert.isNull(await graph.Car.find(id))
    })
  })

  describe('#findBy', () => {
    it('findBy returns all fields', async () => {
      const model = 'foo'
      const { id } = await graph.Car.create({ model, make: 'foo' })
      const car = await graph.Car.findBy('model', model)
      assert.containSubset(car, {
        id,
        _nodeType: 'Car',
        model,
      })
      assert.property(car, 'updatedAt')
      assert.property(car, 'createdAt')
    })

    it('returns null when nothing exists', async () => {
      const node = await graph.Car.findBy('model', 'asdsadas')
      assert.isNull(node)
    })
  })

  describe('#FINDBY', () => {
    it('findBy returns all fields', async () => {
      const model = 'foo'
      const { id } = await graph.Car.create({ model, make: 'foo' })
      const car = await graph.Car.FINDBY('model', model)
      assert.containSubset(car, {
        id,
        _nodeType: 'Car',
        model,
      })
      assert.property(car, 'updatedAt')
      assert.property(car, 'createdAt')
    })

    it('throws if nothing exists', async () => {
      await assert.isRejected(graph.Car.FINDBY('model', 'asdsadas'), 'Car.model:"asdsadas" does not exist')
    })
  })

  describe('#findByRange', () => {
    it('returns an empty array when looking up by range', async () => {
      await graph.Car.create({ make: 'foo', model: 'foo', nextMaintenance: Date.now() - 1000 })
      const nodes = await graph.Car.findByRange('nextMaintenance', Date.now(), Date.now() + 10000)
      assert.lengthOf(nodes, 0)
    })

    it('returns an array when looking up by range', async () => {
      await graph.Car.create({ make: 'foo', model: 'foo', nextMaintenance: Date.now() - 1000 })
      await graph.Car.create({ make: 'foo', model: 'foo', nextMaintenance: Date.now() - 1000 })
      const nodes = await graph.Car.findByRange('nextMaintenance', 0, Date.now())
      assert.lengthOf(nodes, 2)
    })
  })

  describe('.create', () => {
    it('creates a typed node', async () => {
      const node = await graph.Car.create({ model: 'new!', make: 'old' })
      const id = String(await redis.get('graph:counter:id'))
      assert.equal(node.id, id)
      assert.equal(node.model, 'new!')
      assert.equal(await redis.zscore('car:index:id', node.id), 0)
    })

    it('cannot create a node if that id is already in use', async () => {
      await graph.Car.create({ make: 'sfd', model: 'new!' })
      await redis.decr('graph:counter:id')
      await assert.isRejected(graph.Car.create({ make: 'sdf', model: 'new!' }), /Node:\d+ already exists/)
    })

    it('creates a typed node with createdAt overridable', async () => {
      const node = await graph.Car.create({ make: 'sfd', model: 'new!', createdAt: 999 })
      assert.equal(node.createdAt, 999)
    })

    it(`adds a node's information to its interfaces indexed fields`, async () => {
      const car = await graph.Car.create({ make: 'sdf', model: 'new!' })
      const Vehicle = graph.Car.typeDef.interfaces[1]
      const lowerInterfaceName = Vehicle.name.toLowerCase()

      const preDeleteCarIdScore = await redis.zscore(`${lowerInterfaceName}:index:id`, car.id)
      assert.equal(preDeleteCarIdScore, 0)
    })
  })

  describe('#update', () => {
    it('updates a typed node', async () => {
      const { id } = await graph.Passenger.create({ name: 'new!' })
      const node = await graph.Passenger.update({ id, name: 'baz' })
      assert.containSubset(node, { id, name: 'baz' })
    })

    it('rejects if you try and update a node without supplying an id', async () => {
      await assert.isRejected(graph.Passenger.update({ title: 'baz' }), /"update.id" is required/)
    })

    it('rejects if you try and update a node that does not exist', async () => {
      await assert.isRejected(graph.Passenger.update({ id: 1000, title: 'baz' }), /Passenger:\d+ does not exist/)
    })

    it('rejects if you try to change the nodeType of a node', async () => {
      const { id } = await graph.Passenger.create({ name: 'new!' })
      await assert.isRejected(graph.Passenger.update({ id, _nodeType: 'Car' }), /Cannot change \w+:\d+ to a \w+/)
    })

    it('adds to index when the previous value was undefined/null', async () => {
      const { id } = await graph.Passenger.create({ name: 'Trey' })
      await graph.Passenger.update({ id, name: 'baz' })
      const baz = await graph.Passenger.findBy('name', 'baz')
      assert.equal(baz.id, id)
    })

    it('removes null/undefined values from index on update', async () => {
      const { id } = await graph.Passenger.create({ name: 'foo', nickname: 'foo' })
      await graph.Passenger.update({ id, nickname: null })
      const passengerByOldName = await graph.Passenger.findBy('nickname', 'foo')
      assert.isNull(passengerByOldName)
    })

    it('removes old values from index on update', async () => {
      const { id } = await graph.Passenger.create({ name: 'foo' })
      await graph.Passenger.update({ id, name: 'baz' })
      const passengerByOldName = await graph.Passenger.findBy('name', 'foo')
      assert.isNull(passengerByOldName)
    })

    it(`adds to interfaces' indexes when the previous value was undefined/null`, async () => {
      const { id } = await graph.Car.create({ make: 'GraphCar', model: 'GO!' })
      const nextMaintenance = Date.now()
      await graph.Car.update({ id, nextMaintenance })
      const [vehicle] = await graph.Vehicle.findByRange('nextMaintenance', nextMaintenance, nextMaintenance + 1)
      assert.equal(vehicle.id, id)
    })

    it(`removes null/undefined values from interfaces' indexes on update`, async () => {
      const nextMaintenance = Date.now()
      const { id } = await graph.Car.create({ make: 'hi', model: 'bye', nextMaintenance })
      await graph.Car.update({ id, nextMaintenance: null })
      const [vehicle] = await graph.Vehicle.findByRange('nextMaintenance', nextMaintenance, nextMaintenance + 1)
      assert.isUndefined(vehicle)
    })

    it(`removes old values from interfaces' indexes on update`, async () => {
      const nextMaintenance = Date.now()
      const { id } = await graph.Car.create({ make: 'hi', model: 'bye', nextMaintenance })
      await graph.Car.update({ id, nextMaintenance: nextMaintenance + 1000 })
      const [vehicle] = await graph.Vehicle.findByRange('nextMaintenance', nextMaintenance, nextMaintenance + 1)
      assert.isUndefined(vehicle)
    })
  })

  describe('#count', () => {
    beforeEach(async () => {
      await Promise.all([
        graph.Car.create({ make: 'hi', model: 'bye', nextMaintenance: 1 }),
        graph.Car.create({ make: 'hi', model: 'bye', nextMaintenance: 2 }),
        graph.Car.create({ make: 'hi', model: 'bye', nextMaintenance: 3 }),
        graph.Car.create({ make: 'hi', model: 'bye', nextMaintenance: null }),
      ])
    })

    it('counts all cars', async () => {
      const count = await graph.Car.count()
      assert.equal(count, 4)
    })
  })

  describe('#delete', () => {
    let context: any
    beforeEach(async () => {
      const cars = await Promise.all([
        graph.Car.create({ make: 'car', model: 'type-a', nextMaintenance: 1 }),
        graph.Car.create({ make: 'car', model: 'type-b', nextMaintenance: 2 }),
        graph.Car.create({ make: 'car', model: 'type-c', nextMaintenance: 3 }),
        graph.Car.create({ make: 'car', model: 'type-?', nextMaintenance: null }),
      ])
      const passengers = await Promise.all([
        graph.Passenger.create({ name: 'smoke' }),
        graph.Passenger.create({ name: 'ice' }),
      ])
      await Promise.all([
        graph.edge.create({
          subject: cars[0],
          object: passengers[0],
          predicate,
          weight: 30,
        }),
        graph.edge.create({
          subject: cars[1],
          object: passengers[0],
          predicate,
          weight: 10,
        }),
        graph.edge.create({
          subject: cars[2],
          object: passengers[0],
          predicate,
          weight: 20,
        }),
      ])

      context = { cars, passengers }
    })

    it('should delete the node', async () => {
      const {
        cars: [car],
      } = context
      const preDeleteKeys = (await redis.keys('*')) as string[]
      const nodesPreDeleteKeys = preDeleteKeys.filter(k => k.split(':').includes(car.id))
      assert.ok(nodesPreDeleteKeys.length)

      const node = await graph.Car.delete(car.id)
      assert.containSubset(node, { id: car.id })

      const postDeleteKeys = (await redis.keys('*')) as string[]
      const nodesPostDeleteKeys = postDeleteKeys.filter(k => k.split(':').includes(car.id))
      assert.equal(nodesPostDeleteKeys.length, 0)
    })

    it('takes a number', async () => {
      const {
        cars: [car],
      } = context
      const preDeleteKeys = (await redis.keys('*')) as string[]
      const nodesPreDeleteKeys = preDeleteKeys.filter(k => k.split(':').includes(car.id))
      assert.ok(nodesPreDeleteKeys.length)

      const node = await graph.Car.delete(Number(car.id))
      assert.containSubset(node, { id: car.id })

      const postDeleteKeys: string[] = await redis.keys('*')
      const nodesPostDeleteKeys = postDeleteKeys.filter(k => k.split(':').includes(car.id))
      assert.equal(nodesPostDeleteKeys.length, 0)
    })

    it('should delete all references to that node in any indexes', async () => {
      const {
        cars: [car],
      } = context
      const preDeleteCarId = await redis.hget('car:index:model', car.model)
      const preDeleteCarScore = await redis.zscore('car:index:nextMaintenance', car.id)
      const preDeleteIdScore = await redis.zscore('car:index:id', car.id)
      assert.equal(preDeleteCarId, car.id)
      assert.equal(preDeleteCarScore, car.nextMaintenance)
      assert.equal(preDeleteIdScore, 0)

      await graph.Car.delete(car.id)

      const postDeleteCarId = await redis.hget('car:index:model', car.model)
      const postDeleteCarScore = await redis.zscore('car:index:nextMaintenance', car.id)
      const postDeleteIdScore = await redis.zscore('car:index:id', car.id)
      assert.equal(postDeleteCarScore, null)
      assert.equal(postDeleteCarId, null)
      assert.equal(postDeleteIdScore, null)
    })

    it('should delete all edges of that node', async () => {
      const {
        cars: [car],
      } = context
      const preDeleteIndexExists = await redis.exists(`e:w:s:${car.id}:${predicate}`)
      assert.ok(preDeleteIndexExists)

      await graph.Car.delete(car.id)

      const postDeleteIndexExists = await redis.exists(`e:w:s:${car.id}:${predicate}`)
      assert.notOk(postDeleteIndexExists)
    })

    it('should delete all the labeled edges of that node', async () => {
      const predicate = 'SerialNumber'
      const carModel = await graph.CarModel.create({ name: 'Camry' })
      const value = '1234567890'
      const {
        cars: [car],
      } = context

      await graph.labeledEdge.create({
        subject: carModel,
        object: car,
        predicate,
        value,
      })

      assert.equal(await redis.exists(`e:l:${carModel.id}:${predicate}`), 1)
      assert.equal(await redis.exists(`e:l:_o_e_:${car.id}`), 1)

      await graph.Car.delete(car.id)

      assert.equal(await redis.exists(`e:l:${carModel.id}:${predicate}`), 0)
      assert.equal(await redis.exists(`e:l:_o_e_:${car.id}`), 0)
    })

    it(`should not be able to add a node to an index while it's being deleted`, async () => {
      const { cars } = context
      const car = cars[3]

      // simulate the delete being in progress
      await redis.hmset(`node:fields:${car.id}`, { _state: STATES.DELETING })
      await assert.isRejected(
        graph.Car.update({ id: car.id, nextMaintenance: 3 }),
        /Cannot update a node being deleted/
      )
      await assert.isRejected(graph.Car.update({ id: car.id, model: 'type-d' }), /Cannot update a node being deleted/)

      const postDeleteCarId = await redis.hget('car:index:model', 'type-d')
      const postDeleteCarScore = await redis.zscore('car:index:nextMaintenance', car.id)
      assert.equal(postDeleteCarScore, null)
      assert.equal(postDeleteCarId, null)
    })

    it(`should not be able to draw edges to a node while it's being deleted`, async () => {
      const { cars, passengers } = context
      const car = cars[3]

      // simulate the delete being in progress
      await redis.hmset(`node:fields:${car.id}`, { _state: STATES.DELETING })
      await assert.isRejected(
        graph.edge.create({ subject: car, object: passengers[1], predicate }),
        /Subject node with id \d+ does not exist/
      )

      const postDeleteIndexExists = await redis.exists(`e:w:s:${car.id}:${predicate}`)
      assert.notOk(postDeleteIndexExists)
    })

    it(`should not be able to delete twice`, async () => {
      const {
        cars: [car],
      } = context
      await graph.Car.delete(car.id)
      await assert.isRejected(graph.Car.delete(car.id), /Car:\d+ does not exist/)
    })

    it(`deletes dependent nodes`, async () => {
      const {
        cars: [car],
      } = context
      const passenger = await graph.Passenger.create({ name: 'Smokey' })
      await graph.edge.create({ subject: passenger, predicate: RegistrationNumber, object: car, weight: 123456789 })
      await graph.Passenger.delete(passenger.id)
      await assert.isRejected(graph.Car.FIND(car.id), /Car:\d+ does not exist/)
    })
  })

  describe('#scan', () => {
    beforeEach(async () => {
      await Promise.all([
        graph.Car.create({ make: 'foo', model: 'bar', nextMaintenance: 1 }),
        graph.Car.create({ make: 'foo', model: 'bar', nextMaintenance: 2 }),
        graph.Car.create({ make: 'foo', model: 'bar', nextMaintenance: 3 }),
        graph.Car.create({ make: 'foo', model: 'bar', nextMaintenance: 4 }),
      ])
      graph.resetStats()
    })

    it('returns all results', async () => {
      const stream = graph.Car.scan()
      const nodes = (await collectStream(stream)) as GraphDBNode[]
      assert.equal(nodes.length, 4)
      nodes.forEach(data => {
        assert.containSubset(data, { _nodeType: 'Car' })
      })
    })

    it('returns all results and fields', async () => {
      const stream = graph.Car.scan({ fields: true })
      const nodes = (await collectStream(stream)) as GraphDBNode[]
      assert.equal(nodes.length, 4)
      nodes.forEach(data => {
        assert.containSubset(data, { _nodeType: 'Car', make: 'foo' })
      })
    })

    describe('non id indexed fields', () => {
      it('returns all results', async () => {
        const stream = graph.Car.scan({ indexedField: 'nextMaintenance' })
        const nodes = (await collectStream(stream)) as GraphDBNode[]
        assert.equal(nodes.length, 4)
        nodes.forEach(data => {
          assert.containSubset(data, { _nodeType: 'Car' })
        })
      })

      it('returns all results and fields', async () => {
        const stream = graph.Car.scan({ fields: true, indexedField: 'nextMaintenance' })
        const nodes = (await collectStream(stream)) as GraphDBNode[]
        assert.equal(nodes.length, 4)
        nodes.forEach(data => {
          assert.containSubset(data, { _nodeType: 'Car', make: 'foo' })
        })
      })

      it('cannot scan a non numeric indexed field', async () => {
        assert.throw(() => graph.Car.scan({ fields: true, indexedField: 'make' }))
      })
    })
  })

  describe('#batchScan', () => {
    beforeEach(async () => {
      await Promise.all([
        graph.Car.create({ make: 'foo', model: 'bar', nextMaintenance: 1 }),
        graph.Car.create({ make: 'foo', model: 'bar', nextMaintenance: 2 }),
        graph.Car.create({ make: 'foo', model: 'bar', nextMaintenance: 3 }),
        graph.Car.create({ make: 'foo', model: 'bar', nextMaintenance: 4 }),
      ])
      graph.resetStats()
    })

    it('returns all results in batches', async () => {
      const stream = graph.Car.batchScan({ batchSize: 4 })
      const batches = (await collectStream(stream)) as GraphDBNode[][]
      assert.equal(batches.length, 1)
      batches.forEach(nodes => {
        assert.equal(nodes.length, 4)
        assert.containSubset(nodes[0], { _nodeType: 'Car' })
      })
    })

    it('returns nothing if there are no results', async () => {
      const stream = graph.Van.batchScan({ batchSize: 4 })
      const batches = await collectStream(stream)
      assert.isNull(batches)
    })

    it('returns all results and fields in batches', async () => {
      const stream = graph.Car.batchScan({ fields: true })
      const batches = (await collectStream(stream)) as GraphDBNode[][]
      batches[0].forEach(data => {
        assert.containSubset(data, { _nodeType: 'Car', model: 'bar' })
      })
    })

    describe('non id indexed fields', () => {
      it('returns all results in batches', async () => {
        const stream = graph.Car.batchScan({ batchSize: 4, indexedField: 'nextMaintenance' })
        const batches = (await collectStream(stream)) as GraphDBNode[][]
        assert.equal(batches.length, 1)
        batches.forEach(nodes => {
          assert.equal(nodes.length, 4)
          assert.containSubset(nodes[0], { _nodeType: 'Car' })
        })
      })

      it('returns nothing if there are no results', async () => {
        const stream = graph.Van.batchScan({ batchSize: 4, indexedField: 'nextMaintenance' })
        const batches = await collectStream(stream)
        assert.isNull(batches)
      })

      it('returns all results and fields in batches', async () => {
        const stream = graph.Car.batchScan({ fields: true, indexedField: 'nextMaintenance' })
        const batches = (await collectStream(stream)) as GraphDBNode[][]
        batches[0].forEach(data => {
          assert.containSubset(data, { _nodeType: 'Car', model: 'bar' })
        })
      })
      it('cannot batch scan a non numeric indexed field', async () => {
        assert.throw(() => graph.Car.batchScan({ fields: true, indexedField: 'make' }))
      })
    })
  })

  describe('#scanItr', () => {
    beforeEach(async () => {
      await Promise.all([
        graph.Car.create({ make: 'foo', model: 'bar', nextMaintenance: 1 }),
        graph.Car.create({ make: 'foo', model: 'bar', nextMaintenance: 2 }),
        graph.Car.create({ make: 'foo', model: 'bar', nextMaintenance: 3 }),
        graph.Car.create({ make: 'foo', model: 'bar', nextMaintenance: 4 }),
      ])
      graph.resetStats()
    })

    it('returns all results', async () => {
      const itr = graph.Car.scanItr()
      const nodes = await collectItr(itr)
      assert.equal(nodes.length, 4)
      nodes.forEach(data => {
        assert.containSubset(data, { _nodeType: 'Car' })
      })
    })

    it('returns all results and fields', async () => {
      const itr = graph.Car.scanItr({ fields: true })
      const nodes = await collectItr(itr)
      assert.equal(nodes.length, 4)
      nodes.forEach(data => {
        assert.containSubset(data, { _nodeType: 'Car', make: 'foo' })
      })
    })

    describe('non id indexed fields', () => {
      it('returns all results', async () => {
        const itr = graph.Car.scanItr({ indexedField: 'nextMaintenance' })
        const nodes = await collectItr(itr)
        assert.equal(nodes.length, 4, 'got 4 nodes')
        nodes.forEach(data => {
          assert.containSubset(data, { _nodeType: 'Car' })
        })
      })

      it('returns all results and fields', async () => {
        const itr = graph.Car.scanItr({ fields: true, indexedField: 'nextMaintenance' })
        const nodes = await collectItr(itr)
        assert.equal(nodes.length, 4)
        nodes.forEach(data => {
          assert.containSubset(data, { _nodeType: 'Car', make: 'foo' })
        })
      })

      it('cannot scan a non numeric indexed field', async () => {
        await assert.isRejected(graph.Car.scanItr({ fields: true, indexedField: 'make' }).next())
      })
    })
  })

  describe('#batchScanItr', () => {
    beforeEach(async () => {
      await Promise.all([
        graph.Car.create({ make: 'foo', model: 'bar', nextMaintenance: 1 }),
        graph.Car.create({ make: 'foo', model: 'bar', nextMaintenance: 2 }),
        graph.Car.create({ make: 'foo', model: 'bar', nextMaintenance: 3 }),
        graph.Car.create({ make: 'foo', model: 'bar', nextMaintenance: 4 }),
      ])
      graph.resetStats()
    })

    it('returns all results in batches', async () => {
      const itr = graph.Car.batchScanItr({ batchSize: 4 }) as Iterable<GraphDBNode[]>
      const batches = await collectItr(itr)
      assert.equal(batches.length, 1)
      batches.forEach(nodes => {
        assert.equal(nodes.length, 4)
        assert.containSubset(nodes[0], { _nodeType: 'Car' })
      })
    })

    it('returns nothing if there are no results', async () => {
      const itr = graph.Van.batchScanItr({ batchSize: 4 }) as Iterable<GraphDBNode[]>
      const batches = await collectItr(itr)
      assert.equal(batches.length, 0)
    })

    it('returns all results and fields in batches', async () => {
      const itr = graph.Car.batchScanItr({ fields: true }) as Iterable<GraphDBNode[]>
      const batches = await collectItr(itr)
      batches[0].forEach(data => {
        assert.containSubset(data, { _nodeType: 'Car', model: 'bar' })
      })
    })

    describe('non id indexed fields', () => {
      it('returns all results in batches', async () => {
        const itr = graph.Car.batchScanItr({ batchSize: 4, indexedField: 'nextMaintenance' }) as Iterable<GraphDBNode[]>
        const batches = await collectItr(itr)
        assert.equal(batches.length, 1)
        batches.forEach(nodes => {
          assert.equal(nodes.length, 4)
          assert.containSubset(nodes[0], { _nodeType: 'Car' })
        })
      })

      it('returns nothing if there are no results', async () => {
        const itr = graph.Van.batchScanItr({ batchSize: 4, indexedField: 'nextMaintenance' }) as Iterable<GraphDBNode[]>
        const batches = await collectItr(itr)
        assert.equal(batches.length, 0)
      })

      it('returns all results and fields in batches', async () => {
        const itr = graph.Car.batchScanItr({ fields: true, indexedField: 'nextMaintenance' }) as Iterable<GraphDBNode[]>
        const batches = await collectItr(itr)
        batches[0].forEach(data => {
          assert.containSubset(data, { _nodeType: 'Car', model: 'bar' })
        })
      })
      it('cannot batch scan a non numeric indexed field', async () => {
        await assert.isRejected(graph.Car.batchScanItr({ fields: true, indexedField: 'make' }).next())
      })
    })
  })

  describe('#paginate', () => {
    const context: any = {}
    beforeEach(async () => {
      const cars = await Promise.all([
        graph.Car.create({ make: 'foo', model: 'bar', nextMaintenance: 1 }),
        graph.Car.create({ make: 'foo', model: 'bar', nextMaintenance: 2 }),
        graph.Car.create({ make: 'foo', model: 'bar', nextMaintenance: 3 }),
        graph.Car.create({ make: 'foo', model: 'bar', nextMaintenance: 4 }),
      ])
      context.cars = cars
      graph.resetStats()
    })

    it('returns all ascending results', async () => {
      let cursor
      for (let i = 0; i < 4; i++) {
        const car = context.cars[i]
        const {
          cursor: nextCursor,
          nodes: [node],
        } = (await graph.Car.paginate({
          field: 'id',
          count: 1,
          cursor,
        })) as { cursor: number; nodes: GraphDBNode[] }
        cursor = nextCursor
        assert.isOk(cursor)
        assert.containSubset(node, { id: car.id, _nodeType: 'Car' })
      }
      const { cursor: nextCursor, nodes } = await graph.Car.paginate({
        field: 'nextMaintenance',
        count: 1,
        cursor,
      })
      assert.equal(nextCursor, null)
      assert.deepEqual(nodes, [])
    })

    it('returns all descending results', async () => {
      let cursor = 0
      const cars = [...context.cars].sort((a, b) => Number(b.id) - Number(a.id))
      for (let i = 0; i < 4; i++) {
        const car = cars[i]
        const {
          cursor: nextCursor,
          nodes: [node],
        } = await graph.Car.paginate({
          order: 'DESC',
          field: 'id',
          count: 1,
          cursor,
        })
        cursor = nextCursor
        assert.isOk(cursor)
        assert.containSubset(node, { id: car.id, _nodeType: 'Car' })
      }
      const { cursor: nextCursor, nodes } = await graph.Car.paginate({
        field: 'nextMaintenance',
        order: 'DESC',
        count: 1,
        cursor,
      })
      assert.equal(nextCursor, null)
      assert.deepEqual(nodes, [])
    })
  })

  describe('#hasInstanceOf', () => {
    it('returns true if the object is an instance of the type', async () => {
      const car = await graph.Car.create({ make: 'GraphCar', model: 'GO!' })
      const passenger = await graph.Passenger.create({ name: 'Francis' })
      assert.isTrue(graph.Car.hasInstanceOf(car))
      assert.isFalse(graph.Car.hasInstanceOf(passenger))
    })
  })
})
