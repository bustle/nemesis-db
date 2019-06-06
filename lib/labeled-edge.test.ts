import { assert } from 'chai'
import { join, map } from 'bluebird'
import { write, pipe } from 'bluestream'
import { RedisLoader } from 'redis-loader/dist'
import { GraphDBNode } from './node-utils'
import { testConnectGraph, SerialNumber as predicate, RegistrationNumber } from './test/graph'

describe('GraphLoader - GraphLoader', () => {
  let graph: any
  let context: { cars: GraphDBNode[]; carModel: GraphDBNode; value: number }
  let redis: RedisLoader
  before(() => {
    graph = testConnectGraph()
    redis = graph._redis
  })

  describe('Labeled Edges', () => {
    beforeEach(async () => {
      await redis.flushdb()
      const [passenger, carModel, ...cars] = await join(
        graph.Passenger.create({ name: 'foo' }),
        graph.CarModel.create({ name: 'Camry' }),
        graph.Car.create({ make: 'GraphCar', model: 'GO!' }),
        graph.Car.create({ make: 'GraphCar', model: 'GO!' }),
        graph.Car.create({ make: 'GraphCar', model: 'GO!' })
      )
      const value = 1234567890
      context = { cars, carModel, value }

      Object.assign(context, { carModel, value })
      await graph.labeledEdge.create({
        subject: cars[0],
        object: passenger,
        predicate: RegistrationNumber,
        value: 1234,
      })
      await map(cars, (car, i) => {
        return graph.labeledEdge.create({
          subject: carModel,
          object: car,
          predicate,
          value: value + i,
        })
      })
    })
    describe('#create', () => {
      it('can create a labeled edge between two nodes', async () => {
        const {
          carModel: { id },
        } = context
        assert.equal(await redis.exists(`e:l:${id}:${predicate}`), 1)
      })
      it("can overwrite an existing value with the same object's value", async () => {
        const {
          carModel,
          value,
          cars: [car],
        } = context
        await assert.isFulfilled(graph.labeledEdge.create({ subject: carModel, predicate, object: car, value }))
      })
      it("cannot overwrite an existing value with a different object's value", async () => {
        const { carModel, value } = context
        const car = await graph.Car.create({ make: 'GraphCar', model: 'GO!' })
        await assert.isRejected(
          graph.labeledEdge.create({ subject: carModel, predicate, object: car, value }),
          /the value \d+ is already in use/
        )
      })
    })
    describe('#find', () => {
      it('can find a labeled edge between two nodes', async () => {
        const {
          cars: [car],
          carModel,
          value,
        } = context
        const { object } = await graph.labeledEdge.find({
          subject: carModel,
          predicate,
          value,
        })

        assert.containSubset(object, {
          id: car.id,
        })
      })
      it('if a labeled edge does not exist find returns null', async () => {
        const { carModel } = context
        const response = await graph.labeledEdge.find({
          subject: carModel,
          predicate,
          value: 'foo',
        })

        assert.equal(response, null)
      })
    })
    describe('#findValues', () => {
      beforeEach(async () => {
        const {
          cars: [car],
          carModel,
          value,
        } = context
        const values = new Array(9).fill(value)
        await map(values, (value, i) => {
          return graph.labeledEdge.create({
            subject: carModel,
            object: car,
            predicate,
            value: value - (i + 1),
          })
        })
      })
      it('can find all the values between two nodes', async () => {
        const {
          cars: [car],
          carModel,
        } = context
        const values = await graph.labeledEdge.findValues({
          subject: carModel,
          predicate,
          object: car,
        })

        assert.equal(values.length, 10)
      })
      it('can paginate through all the values between two nodes', async () => {
        const {
          cars: [car],
          carModel,
        } = context
        const args = {
          subject: carModel,
          predicate,
          object: car,
          limit: 5,
        }
        const firstFiveValues: string[] = await graph.labeledEdge.findValues(args)
        const secondFiveValues = await graph.labeledEdge.findValues({ ...args, offset: 5 })
        assert.equal(firstFiveValues.length, 5)
        assert.equal(secondFiveValues.length, 5)
        firstFiveValues.forEach(value => assert.isFalse(secondFiveValues.includes(value)))
      })
    })
    describe('#exists', () => {
      it('can see if a labeled edge already exists between two nodes', async () => {
        const {
          carModel,
          value,
          cars: [car1, car2],
        } = context
        assert.isTrue(await graph.labeledEdge.exists({ subject: carModel, predicate, value, object: car1 }))
        assert.isFalse(await graph.labeledEdge.exists({ subject: carModel, predicate, value, object: car2 }))
      })
    })
    describe('#inUse', () => {
      it('can see if a labeled edge already in use', async () => {
        const { carModel, value } = context
        assert.isTrue(await graph.labeledEdge.inUse({ subject: carModel, predicate, value }))
        assert.isFalse(await graph.labeledEdge.inUse({ subject: carModel, predicate, value: 9999999999 }))
      })
    })
    describe('#count', () => {
      it('can see how many values a subject has', async () => {
        const { carModel } = context
        const count = await graph.labeledEdge.count({ subject: carModel, predicate })

        assert.equal(count, 3)
      })
    })
    describe('#countValues', () => {
      it('can see how many values object and a subject have', async () => {
        const {
          carModel,
          cars: [car],
        } = context
        const count = await graph.labeledEdge.countValues({ subject: carModel, predicate, object: car })

        assert.equal(count, 1)
      })
    })
    describe('#delete', () => {
      it('can delete a labeled edge between two nodes', async () => {
        const {
          cars: [car],
          carModel,
          value,
        } = context
        const { object } = await graph.labeledEdge.delete({
          subject: carModel,
          predicate,
          value,
        })

        assert.containSubset(object, {
          id: car.id,
        })
      })
    })
    describe('#unindexObject', () => {
      it("deleting an object node deletes it's references to a subject", async () => {
        const {
          cars: [car],
          carModel,
          value,
        } = context

        assert.equal(await redis.exists(`e:l:_o_e_:${car.id}`), 1)
        assert.equal(await redis.exists(`e:l:_s_e_:${car.id}`), 1)
        await graph.labeledEdge.unindexObject({ object: car })
        assert.equal(await redis.exists(`e:l:_o_e_:${car.id}`), 0)
        assert.equal(await redis.exists(`e:l:_s_e_:${car.id}`), 0)
        const response = await graph.labeledEdge.find({ subject: carModel, predicate, value })

        assert.equal(response, null)
      })
    })
    describe('#traverse', () => {
      it(`returns all ${predicate} labeled edges`, async () => {
        let objectCount = 0
        const { carModel, value } = context
        const readStream = graph.labeledEdge.traverse({
          subject: carModel,
          predicate,
          limit: 1,
        })
        const writeStream = write(data => {
          assert.containSubset(data, { value: String(value + objectCount) })
          objectCount++
        })

        await pipe(
          readStream,
          writeStream
        )
        assert.equal(objectCount, 3)
      })
    })
  })
})
