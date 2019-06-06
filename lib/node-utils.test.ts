import { testConnectGraph } from './test/graph'
import { hasInstanceOf, changeNodeType, deserializeFields, serializeFields } from './node-utils'
import { assert } from 'chai'
import { redis } from 'app/globals'

describe('graphdb core', () => {
  let graph: any

  before(() => {
    graph = testConnectGraph()
  })

  beforeEach(async () => {
    graph.resetStats()
  })

  describe('serialization/deserialization', () => {
    it('handles null json values', async () => {
      const car = await graph.Car.create({ make: 'foo', model: 'bar' })
      assert.isNull(car.metadata)
    })
    it('handles invalid saved json', async () => {
      const car = await graph.Car.create({ make: 'foo', model: 'bar' })
      await graph._redis.hmset(`node:fields:${car.id}`, 'metadata', '')
      assert.isNull(car.metadata)
      await graph._redis.hmset(`node:fields:${car.id}`, 'metadata', 'foobar')
      await assert.isRejected(graph.Car.find(car.id), `deserializeAttributes: Invalid JSON for Car(${car.id}).metadata`)
    })
    it('gives the same result', async () => {
      const source = await graph.Car.create({ createdAt: 1, updatedAt: 1, make: 'foo', model: 'sdf' })
      const serialized = serializeFields(graph.Car.typeDef, source)
      const deserialized = deserializeFields(graph.Car.typeDef, serialized)
      assert.deepEqual(source, deserialized)
    })
  })

  describe('missingFieldValue', () => {
    it('reads the default value for strings', async () => {
      const model = await graph.CarModel.create({ name: 'foo' })
      await graph._redis.hdel(`node:fields:${model.id}`, 'name')
      assert.containSubset(await graph.Node.find(model.id), {
        id: model.id,
        name: '',
      })
    })
    it('reads the default value for booleans', async () => {
      const van = await graph.Van.create({ make: '', model: '', marked: false })
      await graph._redis.hdel(`node:fields:${van.id}`, 'marked')
      assert.containSubset(await graph.Node.find(van.id), {
        id: van.id,
        marked: false,
      })
    })
  })

  describe('defaultValue', () => {
    it('replaces undefined values with the default value', async () => {
      const car = await graph.Car.create({ make: 'foo', model: 'bar', isCrashed: undefined })
      assert.equal(car.isCrashed, false)
      const car2 = await graph.Car.create({ make: 'foo', model: 'bar' })
      assert.equal(car2.isCrashed, false)
    })
    it('does not replace null values', async () => {
      const car = await graph.Car.create({ make: 'foo', model: 'bar', passedInspection: null })
      assert.equal(car.passedInspection, null)
    })
    it('lets you have null as default', async () => {
      const car = await graph.Car.create({ make: 'foo', model: 'bar', isRegistered: undefined })
      assert.equal(car.isRegistered, null)
    })
    it('works for json', async () => {
      const user = await graph.User.create({ name: 'ein' })
      assert.containSubset(user, {
        id: user.id,
        loginAttempts: [],
      })
    })
  })

  describe('hasInstanceOf', () => {
    it('can tell if a node is an instance of it', () => {
      assert.equal(hasInstanceOf(graph.Vehicle.typeDef, { _nodeType: 'Car', id: 99999999 }), true)
      assert.equal(hasInstanceOf(graph.Vehicle.typeDef, { _nodeType: 'Foo', id: 99999999 }), false)
    })
    it('can tell if a node implements it', () => {
      assert.equal(hasInstanceOf(graph.Car.typeDef, { _nodeType: 'Car', id: 99999999 }), true)
      assert.equal(hasInstanceOf(graph.Car.typeDef, { _nodeType: 'Foo', id: 99999999 }), false)
    })
  })

  describe('changeNodeType', () => {
    it('throws if you try to change an object to an interface', async () => {
      await assert.isRejected(
        changeNodeType(redis, graph.schema, 1, 'Vehicle'),
        'target type "Vehicle" must be a NodeDefinition'
      )
    })
    it('throws if you try to change an id that does not exist', async () => {
      await assert.isRejected(changeNodeType(redis, graph.schema, 100000, 'Van'), 'No such Node:100000')
    })
    it('throws if you have an invalid node of the target type', async () => {
      const { id } = await graph.Car.create({ make: 'go', model: 'convertable' }, {})
      await assert.isRejected(
        changeNodeType(redis, graph.schema, id, 'Van'),
        'Unable to serialize Van.marked: value is null or undefined and not nullable'
      )
    })

    it('changes node type', async () => {
      const { id } = await graph.Car.create({ make: 'go', model: 'convertable' })
      await changeNodeType(redis, graph.schema, id, 'Van', { marked: true })
      const van = await graph.Van.FIND(id)
      assert.isTrue(graph.Van.hasInstanceOf(van))
      assert.containSubset(van, {
        id,
        marked: true,
      })
    })
    it('updates interface id indexes', async () => {
      const { id } = await graph.Car.create({ make: 'go', model: 'convertable' })
      await changeNodeType(redis, graph.schema, id, 'Van', { marked: true })
      assert.containSubset(await graph.Vehicle.find(id), { id })
      assert.isNull(await graph.Car.find(id))
    })
    it('updates numeric indexes', async () => {
      const { id } = await graph.Car.create({ make: 'go', model: 'convertable', nextMaintenance: 4 })
      await changeNodeType(redis, graph.schema, id, 'Van', { marked: true })
      assert.equal((await graph.Car.findByRange('nextMaintenance', 3, 5)).length, 0)
      assert.containSubset((await graph.Van.findByRange('nextMaintenance', 3, 5))[0], { id })
      assert.containSubset((await graph.Vehicle.findByRange('nextMaintenance', 3, 5))[0], { id })
    })
    it('updates string indexes', async () => {
      const { id } = await graph.Car.create({ make: 'go', model: 'convertable' })
      await changeNodeType(redis, graph.schema, id, 'Van', { marked: true })
      assert.isNull(await graph.Car.findBy('make', 'go'))
      assert.containSubset(await graph.Van.findBy('make', 'go'), { id })
    })
  })
})
