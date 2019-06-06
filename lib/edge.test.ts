import { assert } from 'chai'
import bluebird, { map } from 'bluebird'
import * as bluestream from 'bluestream'
import { RedisLoader } from 'redis-loader/dist'
import connectRedis from 'lib/redis'
import { GraphLoader } from './index'
import { GraphDBNode } from './node-utils'
import { EdgeDefinition, NodeDefinition } from './definitions'
import { GraphEdgeLoader, EdgeObject } from './edge'
import { testConnectGraph, HasDriver as predicate, HasCarPoolBuddy, Passenger, RegistrationNumber } from './test/graph'
import multiplicities, { MANY_TO_MANY, ONE_TO_MANY, MANY_TO_ONE, ONE_TO_ONE, Multiplicity } from './multiplicities'
import { collect } from 'streaming-iterables'

interface TestGraphLoader extends GraphLoader {
  Vertex: any
}

async function setupImplicitGraphTests(redis: RedisLoader) {
  const edgeFields = {
    subjects: ['Node'],
    objects: ['Node'],
    multiplicity: MANY_TO_MANY as Multiplicity,
  }
  const Vertex = new NodeDefinition({
    name: 'Vertex',
    fields: {},
  })
  const AHasB = new EdgeDefinition({ ...edgeFields, name: 'AHasB' })
  const AHasC = new EdgeDefinition({ ...edgeFields, name: 'AHasC' })
  const AHasD = new EdgeDefinition({ ...edgeFields, name: 'AHasD' })
  const BHasA = new EdgeDefinition({ ...edgeFields, name: 'BHasA' })
  const CHasA = new EdgeDefinition({ ...edgeFields, name: 'CHasA' })
  const DHasA = new EdgeDefinition({ ...edgeFields, name: 'DHasA' })
  const graph = new GraphLoader({
    redis,
    nodeDefs: { Vertex },
    edgeDefs: { AHasB, AHasC, AHasD, BHasA, CHasA, DHasA },
    interfaceDefs: {},
  }) as TestGraphLoader
  const a = await graph.Vertex.create({})
  const bs = await Promise.all([graph.Vertex.create({}), graph.Vertex.create({}), graph.Vertex.create({})])
  const cs = await Promise.all([graph.Vertex.create({}), graph.Vertex.create({}), graph.Vertex.create({})])
  const ds = await Promise.all([graph.Vertex.create({}), graph.Vertex.create({}), graph.Vertex.create({})])
  return { graph, a, bs, cs, ds, AHasB, AHasC, AHasD, BHasA, CHasA, DHasA }
}

describe('GraphLoader - GraphEdgeLoader', () => {
  let Edge: GraphEdgeLoader
  let redis: RedisLoader
  let graph: any
  let context: { car: GraphDBNode; passengers: GraphDBNode[]; drivers: GraphDBNode[] }
  before(() => {
    redis = connectRedis()
    graph = testConnectGraph()
    Edge = graph.edge
  })
  after(() => {
    redis.disconnect()
  })
  beforeEach(async () => {
    await redis.flushdb()
    const car = await graph.Car.create({ make: 'ford', model: 'gtfo' })
    const passengers = await Promise.all([
      graph.Passenger.create({ name: 'Ein' }),
      graph.Passenger.create({ name: 'Ein' }),
      graph.Passenger.create({ name: 'Ein' }),
    ])

    context = { car, passengers, drivers: [] }

    await Promise.all([
      Edge.create({
        subject: car,
        object: passengers[0],
        predicate,
        weight: 30,
      }),
      Edge.create({
        subject: car,
        object: passengers[1],
        predicate,
        weight: 10,
      }),
      Edge.create({
        subject: car,
        object: passengers[2],
        predicate,
        weight: 20,
      }),
    ])
  })

  describe('#create', () => {
    it("errors when nodes don't exist", async () => {
      await assert.isRejected(
        graph.edge.create({ predicate, subject: { id: 99999 }, object: { id: 99999 } }),
        /Node:99999 does not exist/
      )
    })
    it('cannot create an edge between when the subject is the incorrect type', async () => {
      const {
        passengers: [passenger0, passenger1],
      } = context
      await assert.isRejected(
        graph.edge.create({ predicate, subject: passenger0, object: passenger1 }),
        /Subject with nodeType: Passenger is invalid/
      )
    })
    describe('multiplicities', () => {
      let Edge: GraphEdgeLoader
      let graph: any
      let context: { passengers: GraphDBNode[] }
      before(async () => {
        const edgeDefs: { [index: string]: EdgeDefinition } = {}
        multiplicities.forEach((multiplicity: Multiplicity) => {
          edgeDefs[multiplicity] = new EdgeDefinition({
            name: multiplicity,
            subjects: ['Passenger'],
            objects: ['Passenger'],
            multiplicity,
          })
        })
        graph = new GraphLoader({
          redis,
          nodeDefs: { Passenger },
          edgeDefs,
          interfaceDefs: {},
        })
        Edge = graph.edge
      })
      beforeEach(async () => {
        const passengers = await bluebird.join(
          graph.Passenger.create({ name: 'Ein' }),
          graph.Passenger.create({ name: 'Ein' }),
          graph.Passenger.create({ name: 'Ein' })
        )
        context = { passengers }
      })
      it('MANY_TO_MANY', async () => {
        const {
          passengers: [passenger1, passenger2, passenger3],
        } = context
        await assert.isFulfilled(Edge.create({ subject: passenger1, object: passenger2, predicate: MANY_TO_MANY }))
        await assert.isFulfilled(Edge.create({ subject: passenger1, object: passenger3, predicate: MANY_TO_MANY }))
        await assert.isFulfilled(Edge.create({ subject: passenger3, object: passenger2, predicate: MANY_TO_MANY }))
      })
      it('ONE_TO_MANY', async () => {
        const {
          passengers: [passenger1, passenger2, passenger3],
        } = context
        await assert.isFulfilled(Edge.create({ subject: passenger1, object: passenger2, predicate: ONE_TO_MANY }))
        await assert.isFulfilled(Edge.create({ subject: passenger1, object: passenger3, predicate: ONE_TO_MANY }))
        await assert.isRejected(
          Edge.create({ subject: passenger3, object: passenger2, predicate: ONE_TO_MANY }),
          /"object" with id: \d+ already has an edge with "predicate": ONE_TO_MANY/
        )
      })
      it('MANY_TO_ONE', async () => {
        const {
          passengers: [passenger1, passenger2, passenger3],
        } = context
        await assert.isFulfilled(Edge.create({ subject: passenger1, object: passenger2, predicate: MANY_TO_ONE }))
        await assert.isRejected(
          Edge.create({ subject: passenger1, object: passenger3, predicate: MANY_TO_ONE }),
          /"subject" with id: \d+ already has an edge with "predicate": MANY_TO_ONE/
        )
        await assert.isFulfilled(Edge.create({ subject: passenger3, object: passenger2, predicate: MANY_TO_ONE }))
      })
      it('ONE_TO_ONE', async () => {
        const {
          passengers: [passenger1, passenger2, passenger3],
        } = context
        await assert.isFulfilled(Edge.create({ subject: passenger1, object: passenger2, predicate: ONE_TO_ONE }))
        await assert.isRejected(
          Edge.create({ subject: passenger1, object: passenger3, predicate: ONE_TO_ONE }),
          /"subject" with id: \d+ already has an edge with "predicate": ONE_TO_ONE/
        )
        await assert.isRejected(
          Edge.create({ subject: passenger3, object: passenger2, predicate: ONE_TO_ONE }),
          /"object" with id: \d+ already has an edge with "predicate": ONE_TO_ONE/
        )
      })
    })
  })

  describe('#exists', () => {
    it('creates binary hexastore edges', async () => {
      const {
        car: subject,
        passengers: [object],
      } = context
      assert.isTrue(await Edge.exists({ subject, object, predicate }))
    })
  })

  describe('#find', () => {
    it('can find subject edges ordered by weight', async () => {
      const {
        car,
        passengers: [passenger0, passenger1, passenger2],
      } = context
      const edges = await Edge.find({ subject: car, predicate })

      assert.containSubset(edges, [
        { subject: { id: car.id }, predicate: `${predicate}`, object: { id: passenger1.id }, weight: 10 },
        { subject: { id: car.id }, predicate: `${predicate}`, object: { id: passenger2.id }, weight: 20 },
        { subject: { id: car.id }, predicate: `${predicate}`, object: { id: passenger0.id }, weight: 30 },
      ])
    })

    it('can find object edges ordered by weight', async () => {
      const {
        car,
        passengers: [passenger],
      } = context
      const edges = await Edge.find({ object: passenger, predicate })
      assert.containSubset([{ subject: car, predicate: `${predicate}`, object: passenger, weight: 30 }], edges)
    })

    describe('implicit finding', () => {
      let graph: TestGraphLoader
      describe('subject only', () => {
        let A: GraphDBNode
        let Bs: GraphDBNode[]
        let Cs: GraphDBNode[]
        let Ds: GraphDBNode[]
        beforeEach(async () => {
          const { a, bs, cs, ds, graph: setupGraph } = await setupImplicitGraphTests(redis)
          graph = setupGraph
          A = a
          Bs = bs
          Cs = cs
          Ds = ds
          // dummy data to be ignored
          await graph.edge.create({ object: a, predicate: 'BHasA', subject: Bs[0] })
        })
        it('can retrieve edges', async () => {
          const edges = await map(Bs, b => graph.edge.create({ subject: A, predicate: 'AHasB', object: b }))
          const foundEdges = await graph.edge.find({ subject: A })

          assert.equal(foundEdges.length, 3)
          assert.containSubset(foundEdges, edges)
        })
        it('can retrieve edges with multiple predicates', async () => {
          const bEdges = await map(Bs, b => graph.edge.create({ subject: A, predicate: 'AHasB', object: b }))
          const cEdges = await map(Cs, c => graph.edge.create({ subject: A, predicate: 'AHasC', object: c }))
          const dEdges = await map(Ds, d => graph.edge.create({ subject: A, predicate: 'AHasD', object: d }))
          const edges = [...bEdges, ...cEdges, ...dEdges]

          const foundEdges = await graph.edge.find({ subject: A })
          assert.containSubset(foundEdges, edges)
          assert.equal(foundEdges.length, edges.length)
        })
        it('can retrieve edges with multiple predicates and an offset', async () => {
          await map(Bs, b => graph.edge.create({ subject: A, predicate: 'AHasB', object: b }))
          const cEdges = await map(Cs, c => graph.edge.create({ subject: A, predicate: 'AHasC', object: c }))
          const dEdges = await map(Ds, d => graph.edge.create({ subject: A, predicate: 'AHasD', object: d }))

          const edges = [cEdges[1], cEdges[2], ...dEdges]
          const foundEdges = await graph.edge.find({ subject: A, offset: 4 })
          assert.equal(foundEdges.length, edges.length)
          assert.containSubset(foundEdges, edges)
        })
        it('can retrieve edges with multiple predicates, offset, and a limit', async () => {
          const bEdges = await map(Bs, b => graph.edge.create({ subject: A, predicate: 'AHasB', object: b }))
          const cEdges = await map(Cs, c => graph.edge.create({ subject: A, predicate: 'AHasC', object: c }))
          const dEdges = await map(Ds, d => graph.edge.create({ subject: A, predicate: 'AHasD', object: d }))

          const edges = [bEdges[2], ...cEdges, dEdges[0]]
          const foundEdges = await graph.edge.find({ subject: A, offset: 2, limit: 5 })
          assert.equal(foundEdges.length, edges.length)
          assert.containSubset(foundEdges, edges)
        })
      })
      describe('object only', () => {
        let A: GraphDBNode
        let Bs: GraphDBNode[]
        let Cs: GraphDBNode[]
        let Ds: GraphDBNode[]
        beforeEach(async () => {
          const { a, bs, cs, ds, graph: setupGraph } = await setupImplicitGraphTests(redis)
          graph = setupGraph
          A = a
          Bs = bs
          Cs = cs
          Ds = ds
          // dummy data to be ignored
          await graph.edge.create({ subject: a, predicate: 'AHasB', object: Bs[0] })
        })
        it('can retrieve edges', async () => {
          const edges = await map(Bs, b => graph.edge.create({ object: A, predicate: 'BHasA', subject: b }))
          const foundEdges = await graph.edge.find({ object: A })

          assert.equal(foundEdges.length, edges.length)
          assert.containSubset(foundEdges, edges)
        })
        it('can retrieve edges with multiple predicates', async () => {
          const bEdges = await map(Bs, b => graph.edge.create({ object: A, predicate: 'BHasA', subject: b }))
          const cEdges = await map(Cs, c => graph.edge.create({ object: A, predicate: 'CHasA', subject: c }))
          const dEdges = await map(Ds, d => graph.edge.create({ object: A, predicate: 'DHasA', subject: d }))
          const edges = [...bEdges, ...cEdges, ...dEdges]

          const foundEdges = await graph.edge.find({ object: A })
          assert.containSubset(foundEdges, edges)
          assert.equal(foundEdges.length, edges.length)
        })
        it('can retrieve edges with multiple predicates and an offset', async () => {
          await map(Bs, b => graph.edge.create({ object: A, predicate: 'BHasA', subject: b }))
          const cEdges = await map(Cs, c => graph.edge.create({ object: A, predicate: 'CHasA', subject: c }))
          const dEdges = await map(Ds, d => graph.edge.create({ object: A, predicate: 'DHasA', subject: d }))

          const edges = [cEdges[1], cEdges[2], ...dEdges]
          const foundEdges = await graph.edge.find({ object: A, offset: 4 })
          assert.equal(foundEdges.length, edges.length)
          assert.containSubset(foundEdges, edges)
        })
        it('can retrieve edges with multiple predicates, offset, and a limit', async () => {
          const bEdges = await map(Bs, b => graph.edge.create({ object: A, predicate: 'BHasA', subject: b }))
          const cEdges = await map(Cs, c => graph.edge.create({ object: A, predicate: 'CHasA', subject: c }))
          const dEdges = await map(Ds, d => graph.edge.create({ object: A, predicate: 'DHasA', subject: d }))

          const edges = [bEdges[2], ...cEdges, dEdges[0]]
          const foundEdges = await graph.edge.find({ object: A, offset: 2, limit: 5 })
          assert.equal(foundEdges.length, edges.length)
          assert.containSubset(foundEdges, edges)
        })
      })
      describe('predicate only', () => {
        let A: GraphDBNode
        let Bs: GraphDBNode[]
        let edges: EdgeObject[]
        beforeEach(async () => {
          const { a, graph: setupGraph } = await setupImplicitGraphTests(redis)
          graph = setupGraph
          A = a
          Bs = await Promise.all([
            graph.Vertex.create({}),
            graph.Vertex.create({}),
            graph.Vertex.create({}),
            graph.Vertex.create({}),
            graph.Vertex.create({}),
            graph.Vertex.create({}),
            graph.Vertex.create({}),
            graph.Vertex.create({}),
            graph.Vertex.create({}),
          ])
          edges = await map(Bs, b => graph.edge.create({ subject: A, predicate: 'AHasB', object: b }))
          // dummy data to be ignored
          await graph.edge.create({ object: A, predicate: 'BHasA', subject: Bs[0] })
        })
        it('can retrieve edges', async () => {
          const foundEdges = await graph.edge.find({ predicate: 'AHasB' })

          assert.equal(foundEdges.length, edges.length)
          assert.containSubset(foundEdges, edges)
        })
        it('can retrieve edges with an offset', async () => {
          const foundEdges = await graph.edge.find({ predicate: 'AHasB', offset: 4 })
          edges = edges.slice(4, 9)

          assert.equal(foundEdges.length, edges.length)
          assert.containSubset(foundEdges, edges)
        })
        it('can retrieve edges with an offset and a limit', async () => {
          const foundEdges = await graph.edge.find({ predicate: 'AHasB', offset: 2, limit: 5 })
          edges = edges.slice(2, 7)

          assert.equal(foundEdges.length, edges.length)
          assert.containSubset(foundEdges, edges)
        })
      })
      describe('subject and object', () => {
        let subject: GraphDBNode
        let object: GraphDBNode
        let predicates: string[]
        let edges: EdgeObject[]
        beforeEach(async () => {
          const { graph: setupGraph, a, bs } = await setupImplicitGraphTests(redis)
          graph = setupGraph
          subject = await graph.Vertex.create({})
          object = await graph.Vertex.create({})
          predicates = ['AHasB', 'AHasC', 'AHasD']
          edges = await map(predicates, predicate => graph.edge.create({ subject, predicate, object }))
          // dummy data to be ignored
          await graph.edge.create({ subject: a, predicate: 'AHasB', object: bs[0] })
        })
        it('can retrieve edges', async () => {
          const foundEdges = await graph.edge.find({ subject, object })
          assert.equal(foundEdges.length, edges.length)
          assert.containSubset(foundEdges, edges)
        })
        it('can retrieve edges with multiple predicates and an offset', async () => {
          const foundEdges = await graph.edge.find({ subject, object, offset: 1 })
          edges = edges.slice(1, 3)

          assert.equal(foundEdges.length, edges.length)
          assert.containSubset(foundEdges, edges)
        })
        it('can retrieve edges with multiple predicates, an offset, and a limit', async () => {
          const foundEdges = await graph.edge.find({ subject, object, offset: 1, limit: 1 })
          edges = edges.slice(1, 2)

          assert.equal(foundEdges.length, edges.length)
          assert.containSubset(foundEdges, edges)
        })
      })
      describe('subject and predicate', () => {
        let A: GraphDBNode
        let Bs: GraphDBNode[]
        let edges: EdgeObject[]
        beforeEach(async () => {
          const { a, bs, graph: setupGraph } = await setupImplicitGraphTests(redis)
          graph = setupGraph
          A = a
          Bs = bs
          edges = await map(Bs, b => graph.edge.create({ subject: A, predicate: 'AHasB', object: b }))
          // dummy data to be ignored
          await graph.edge.create({ object: A, predicate: 'BHasA', subject: Bs[0] })
        })
        it('can retrieve edges', async () => {
          const foundEdges = await graph.edge.find({ subject: A, predicate: 'AHasB' })

          assert.equal(foundEdges.length, edges.length)
          assert.containSubset(foundEdges, edges)
        })
        it('can retrieve edges with an offset', async () => {
          const foundEdges = await graph.edge.find({ subject: A, predicate: 'AHasB', offset: 1 })
          edges = edges.slice(1, 3)

          assert.equal(foundEdges.length, edges.length)
          assert.containSubset(foundEdges, edges)
        })
        it('can retrieve edges with an offset and a limit', async () => {
          const foundEdges = await graph.edge.find({ subject: A, predicate: 'AHasB', offset: 1, limit: 1 })
          edges = edges.slice(1, 2)

          assert.equal(foundEdges.length, edges.length)
          assert.containSubset(foundEdges, edges)
        })
      })
      describe('object and predicate', () => {
        let A: GraphDBNode
        let Bs: GraphDBNode[]
        let edges: EdgeObject[]
        beforeEach(async () => {
          const { a, bs, graph: setupGraph } = await setupImplicitGraphTests(redis)
          graph = setupGraph
          A = a
          Bs = bs
          edges = await map(Bs, b => graph.edge.create({ object: A, predicate: 'BHasA', subject: b }))
          // dummy data to be ignored
          await graph.edge.create({ subject: A, predicate: 'AHasB', object: Bs[0] })
        })
        it('can retrieve edges', async () => {
          const foundEdges = await graph.edge.find({ object: A, predicate: 'BHasA' })

          assert.equal(foundEdges.length, edges.length)
          assert.containSubset(foundEdges, edges)
        })
        it('can retrieve edges with an offset', async () => {
          const foundEdges = await graph.edge.find({ object: A, predicate: 'BHasA', offset: 1 })
          edges = edges.slice(1, 3)

          assert.equal(foundEdges.length, edges.length)
          assert.containSubset(foundEdges, edges)
        })
        it('can retrieve edges with an offset and a limit', async () => {
          const foundEdges = await graph.edge.find({ object: A, predicate: 'BHasA', offset: 1, limit: 1 })
          edges = edges.slice(1, 2)

          assert.equal(foundEdges.length, edges.length)
          assert.containSubset(foundEdges, edges)
        })
      })
    })
  })

  describe('#count', () => {
    let graph: any
    describe('no predicate, subject, or object', () => {
      let A: GraphDBNode
      let Bs: GraphDBNode[]
      let Cs: GraphDBNode[]
      let Ds: GraphDBNode[]
      beforeEach(async () => {
        const { a, bs, cs, ds, graph: setupGraph } = await setupImplicitGraphTests(redis)
        graph = setupGraph
        A = a
        Bs = bs
        Cs = cs
        Ds = ds
      })
      it('can count edges', async () => {
        await map(Bs, b => graph.edge.create({ subject: A, predicate: 'AHasB', object: b }))
        await map(Cs, c => graph.edge.create({ subject: A, predicate: 'AHasC', object: c }))
        await map(Ds, d => graph.edge.create({ subject: A, predicate: 'AHasD', object: d }))
        await map(Bs, b => graph.edge.create({ object: A, predicate: 'BHasA', subject: b }))
        assert.deepEqual(await graph.edge.count(), 12)
      })
    })
    describe('subject only', () => {
      let A: GraphDBNode
      let Bs: GraphDBNode[]
      let Cs: GraphDBNode[]
      let Ds: GraphDBNode[]
      beforeEach(async () => {
        const { a, bs, cs, ds, graph: setupGraph } = await setupImplicitGraphTests(redis)
        graph = setupGraph
        A = a
        Bs = bs
        Cs = cs
        Ds = ds
      })
      it('can count edges', async () => {
        await map(Bs, b => graph.edge.create({ subject: A, predicate: 'AHasB', object: b }))
        await map(Cs, c => graph.edge.create({ subject: A, predicate: 'AHasC', object: c }))
        await map(Ds, d => graph.edge.create({ subject: A, predicate: 'AHasD', object: d }))
        assert.deepEqual(await graph.edge.count({ subject: A }), 9)
      })
    })
    describe('object only', () => {
      let A: GraphDBNode
      let Bs: GraphDBNode[]
      let Cs: GraphDBNode[]
      let Ds: GraphDBNode[]
      beforeEach(async () => {
        const { a, bs, cs, ds, graph: setupGraph } = await setupImplicitGraphTests(redis)
        graph = setupGraph
        A = a
        Bs = bs
        Cs = cs
        Ds = ds
      })
      it('can count edges', async () => {
        await map(Bs, b => graph.edge.create({ object: A, predicate: 'BHasA', subject: b }))
        await map(Cs, c => graph.edge.create({ object: A, predicate: 'CHasA', subject: c }))
        await map(Ds, d => graph.edge.create({ object: A, predicate: 'DHasA', subject: d }))
        assert.deepEqual(await graph.edge.count({ object: A }), 9)
      })
    })
    describe('predicate only', () => {
      let A: GraphDBNode
      let Bs: GraphDBNode[]
      beforeEach(async () => {
        const { a, graph: setupGraph } = await setupImplicitGraphTests(redis)
        graph = setupGraph
        A = a
        Bs = await Promise.all([
          graph.Vertex.create({}),
          graph.Vertex.create({}),
          graph.Vertex.create({}),
          graph.Vertex.create({}),
          graph.Vertex.create({}),
          graph.Vertex.create({}),
          graph.Vertex.create({}),
          graph.Vertex.create({}),
          graph.Vertex.create({}),
        ])
        await map(Bs, b => graph.edge.create({ subject: A, predicate: 'AHasB', object: b }))
      })
      it('can count edges', async () => {
        assert.equal(await graph.edge.count({ predicate: 'AHasB' }), 9)
      })
    })
    describe('subject and object', () => {
      let subject: GraphDBNode
      let object: GraphDBNode
      let predicates: string[]
      beforeEach(async () => {
        const { graph: setupGraph } = await setupImplicitGraphTests(redis)
        graph = setupGraph
        subject = await graph.Vertex.create({})
        object = await graph.Vertex.create({})
        predicates = ['AHasB', 'AHasC', 'AHasD']
        await map(predicates, predicate => graph.edge.create({ subject, predicate, object }))
      })
      it('can count edges', async () => {
        assert.equal(await graph.edge.count({ subject, object }), 3)
      })
    })
    describe('subject and predicate', () => {
      let A: GraphDBNode
      let Bs: GraphDBNode[]
      beforeEach(async () => {
        const { a, bs, graph: setupGraph } = await setupImplicitGraphTests(redis)
        graph = setupGraph
        A = a
        Bs = bs
        await map(Bs, b => graph.edge.create({ subject: A, predicate: 'AHasB', object: b }))
      })
      it('can count edges', async () => {
        assert.equal(await graph.edge.count({ subject: A, predicate: 'AHasB' }), 3)
      })
    })
    describe('object and predicate', () => {
      let A: GraphDBNode
      let Bs: GraphDBNode[]
      beforeEach(async () => {
        const { a, bs, graph: setupGraph } = await setupImplicitGraphTests(redis)
        graph = setupGraph
        A = a
        Bs = bs
        await map(Bs, b => graph.edge.create({ object: A, predicate: 'BHasA', subject: b }))
      })
      it('can count edges', async () => {
        assert.equal(await graph.edge.count({ object: A, predicate: 'BHasA' }), 3)
      })
    })
  })

  describe('#findNodes', () => {
    it('can find all connected nodes with a subject', async () => {
      const { car, passengers } = context
      const connectedPassengers = await Edge.findNodes({ subject: car, predicate })

      assert.containSubset(connectedPassengers, passengers)
    })
    it('can find all connected nodes with an object', async () => {
      const {
        car,
        passengers: [passenger],
      } = context
      const cars = await Edge.findNodes({ object: passenger, predicate })

      assert.containSubset(cars, [car])
    })
  })

  describe('#traverseItr', () => {
    let graph: any
    describe('no predicate, subject, or object', () => {
      let A: GraphDBNode
      let Bs: GraphDBNode[]
      let Cs: GraphDBNode[]
      let Ds: GraphDBNode[]
      beforeEach(async () => {
        const { a, bs, cs, ds, graph: setupGraph } = await setupImplicitGraphTests(redis)
        graph = setupGraph
        A = a
        Bs = bs
        Cs = cs
        Ds = ds
      })
      it('can retrieve edges', async () => {
        const bEdges = await map(Bs, b => graph.edge.create({ subject: A, predicate: 'AHasB', object: b }))
        const cEdges = await map(Cs, c => graph.edge.create({ subject: A, predicate: 'AHasC', object: c }))
        const dEdges = await map(Ds, d => graph.edge.create({ subject: A, predicate: 'AHasD', object: d }))
        const edges = [...bEdges, ...cEdges, ...dEdges]
        const traversedEdges = await collect(graph.edge.traverseItr({ subject: A }))

        assert.equal(traversedEdges.length, edges.length)
        assert.containSubset(traversedEdges, edges)
      })
    })
    describe('subject only', () => {
      let A: GraphDBNode
      let Bs: GraphDBNode[]
      let Cs: GraphDBNode[]
      let Ds: GraphDBNode[]
      beforeEach(async () => {
        const { a, bs, cs, ds, graph: setupGraph } = await setupImplicitGraphTests(redis)
        graph = setupGraph
        A = a
        Bs = bs
        Cs = cs
        Ds = ds
      })
      it('can retrieve edges', async () => {
        const edges = await map(Bs, b => graph.edge.create({ subject: A, predicate: 'AHasB', object: b }))
        const traversedEdges = await collect(graph.edge.traverseItr({ subject: A }))

        assert.equal(traversedEdges.length, edges.length)
        assert.containSubset(traversedEdges, edges)
      })
      it('can retrieve edges with multiple predicates', async () => {
        const bEdges = await map(Bs, b => graph.edge.create({ subject: A, predicate: 'AHasB', object: b }))
        const cEdges = await map(Cs, c => graph.edge.create({ subject: A, predicate: 'AHasC', object: c }))
        const dEdges = await map(Ds, d => graph.edge.create({ subject: A, predicate: 'AHasD', object: d }))
        const edges = [...bEdges, ...cEdges, ...dEdges]

        const traversedEdges = await collect(graph.edge.traverseItr({ subject: A }))

        assert.equal(traversedEdges.length, edges.length)
        assert.containSubset(traversedEdges, edges)
      })
    })
    describe('object only', () => {
      let A: GraphDBNode
      let Bs: GraphDBNode[]
      let Cs: GraphDBNode[]
      let Ds: GraphDBNode[]
      beforeEach(async () => {
        const { a, bs, cs, ds, graph: setupGraph } = await setupImplicitGraphTests(redis)
        graph = setupGraph
        A = a
        Bs = bs
        Cs = cs
        Ds = ds
      })
      it('can retrieve edges', async () => {
        const edges = await map(Bs, b => graph.edge.create({ object: A, predicate: 'AHasB', subject: b }))
        const traversedEdges = await collect(graph.edge.traverseItr({ object: A }))

        assert.equal(traversedEdges.length, edges.length)
        assert.containSubset(traversedEdges, edges)
      })
      it('can retrieve edges with multiple predicates', async () => {
        const bEdges = await map(Bs, b => graph.edge.create({ object: A, predicate: 'AHasB', subject: b }))
        const cEdges = await map(Cs, c => graph.edge.create({ object: A, predicate: 'AHasC', subject: c }))
        const dEdges = await map(Ds, d => graph.edge.create({ object: A, predicate: 'AHasD', subject: d }))
        const edges = [...bEdges, ...cEdges, ...dEdges]

        const traversedEdges = await collect(graph.edge.traverseItr({ object: A }))

        assert.equal(traversedEdges.length, edges.length)
        assert.containSubset(traversedEdges, edges)
      })
    })
    describe('predicate only', () => {
      let A: GraphDBNode
      let Bs: GraphDBNode[]
      beforeEach(async () => {
        const { a, graph: setupGraph } = await setupImplicitGraphTests(redis)
        graph = setupGraph
        A = a
        Bs = await Promise.all([
          graph.Vertex.create({}),
          graph.Vertex.create({}),
          graph.Vertex.create({}),
          graph.Vertex.create({}),
          graph.Vertex.create({}),
          graph.Vertex.create({}),
          graph.Vertex.create({}),
          graph.Vertex.create({}),
          graph.Vertex.create({}),
        ])
      })
      it('can retrieve all edges', async () => {
        const edges = await map(Bs, b => graph.edge.create({ subject: A, predicate: 'AHasB', object: b }))
        const traversedEdges = await collect(graph.edge.traverseItr({ predicate: 'AHasB' }))

        assert.equal(traversedEdges.length, edges.length)
        assert.containSubset(traversedEdges, edges)
      })
    })
    describe('subject and object', () => {
      let subject: GraphDBNode
      let object: GraphDBNode
      let predicates: string[]
      beforeEach(async () => {
        const { graph: setupGraph } = await setupImplicitGraphTests(redis)
        graph = setupGraph
        subject = await graph.Vertex.create({})
        object = await graph.Vertex.create({})
        predicates = ['AHasB', 'AHasC', 'AHasD']
      })
      it('can traverse all edges', async () => {
        const edges = await map(predicates, predicate => graph.edge.create({ subject, predicate, object }))
        const traversedEdges = await collect(graph.edge.traverseItr({ subject, object }))

        assert.equal(traversedEdges.length, edges.length)
        assert.containSubset(traversedEdges, edges)
      })
    })
    describe('subject and predicate', () => {
      let A: GraphDBNode
      let Bs: GraphDBNode[]
      beforeEach(async () => {
        const { a, bs, graph: setupGraph } = await setupImplicitGraphTests(redis)
        graph = setupGraph
        A = a
        Bs = bs
      })
      it('can traverse edges', async () => {
        const edges = await map(Bs, b => graph.edge.create({ subject: A, predicate: 'AHasB', object: b }))
        const traversedEdges = await collect(graph.edge.traverseItr({ subject: A, predicate: 'AHasB' }))
        assert.containSubset(traversedEdges, edges)
      })
    })
    describe('object and predicate', () => {
      let A: GraphDBNode
      let Bs: GraphDBNode[]
      beforeEach(async () => {
        const { a, bs, graph: setupGraph } = await setupImplicitGraphTests(redis)
        graph = setupGraph
        A = a
        Bs = bs
      })
      it('can traverse edges', async () => {
        const edges = await map(Bs, b => graph.edge.create({ object: A, predicate: 'BHasA', subject: b }))
        const traversedEdges = await collect(graph.edge.traverseItr({ object: A, predicate: 'BHasA' }))
        assert.containSubset(traversedEdges, edges)
      })
    })
  })

  describe('#traverse', () => {
    let graph: any
    describe('subject only', () => {
      let A: GraphDBNode
      let Bs: GraphDBNode[]
      let Cs: GraphDBNode[]
      let Ds: GraphDBNode[]
      beforeEach(async () => {
        const { a, bs, cs, ds, graph: setupGraph } = await setupImplicitGraphTests(redis)
        graph = setupGraph
        A = a
        Bs = bs
        Cs = cs
        Ds = ds
      })
      it('can retrieve edges', async () => {
        const edges = await map(Bs, b => graph.edge.create({ subject: A, predicate: 'AHasB', object: b }))
        const traversedEdges = (await bluestream.collect(graph.edge.traverse({ subject: A }))) as EdgeDefinition[]

        assert.equal(traversedEdges.length, edges.length)
        assert.containSubset(traversedEdges, edges)
      })
      it('can retrieve edges with multiple predicates', async () => {
        const bEdges = await map(Bs, b => graph.edge.create({ subject: A, predicate: 'AHasB', object: b }))
        const cEdges = await map(Cs, c => graph.edge.create({ subject: A, predicate: 'AHasC', object: c }))
        const dEdges = await map(Ds, d => graph.edge.create({ subject: A, predicate: 'AHasD', object: d }))
        const edges = [...bEdges, ...cEdges, ...dEdges]

        const traversedEdges = (await bluestream.collect(graph.edge.traverse({ subject: A }))) as EdgeDefinition[]

        assert.equal(traversedEdges.length, edges.length)
        assert.containSubset(traversedEdges, edges)
      })
    })
    describe('object only', () => {
      let A: GraphDBNode
      let Bs: GraphDBNode[]
      let Cs: GraphDBNode[]
      let Ds: GraphDBNode[]
      beforeEach(async () => {
        const { a, bs, cs, ds, graph: setupGraph } = await setupImplicitGraphTests(redis)
        graph = setupGraph
        A = a
        Bs = bs
        Cs = cs
        Ds = ds
      })
      it('can retrieve edges', async () => {
        const edges = await map(Bs, b => graph.edge.create({ object: A, predicate: 'AHasB', subject: b }))
        const traversedEdges = (await bluestream.collect(graph.edge.traverse({ object: A }))) as EdgeDefinition[]

        assert.equal(traversedEdges.length, edges.length)
        assert.containSubset(traversedEdges, edges)
      })
      it('can retrieve edges with multiple predicates', async () => {
        const bEdges = await map(Bs, b => graph.edge.create({ object: A, predicate: 'AHasB', subject: b }))
        const cEdges = await map(Cs, c => graph.edge.create({ object: A, predicate: 'AHasC', subject: c }))
        const dEdges = await map(Ds, d => graph.edge.create({ object: A, predicate: 'AHasD', subject: d }))
        const edges = [...bEdges, ...cEdges, ...dEdges]

        const traversedEdges = (await bluestream.collect(graph.edge.traverse({ object: A }))) as EdgeDefinition[]

        assert.equal(traversedEdges.length, edges.length)
        assert.containSubset(traversedEdges, edges)
      })
    })
    describe('predicate only', () => {
      let A: GraphDBNode
      let Bs: GraphDBNode[]
      beforeEach(async () => {
        const { a, graph: setupGraph } = await setupImplicitGraphTests(redis)
        graph = setupGraph
        A = a
        Bs = await Promise.all([
          graph.Vertex.create({}),
          graph.Vertex.create({}),
          graph.Vertex.create({}),
          graph.Vertex.create({}),
          graph.Vertex.create({}),
          graph.Vertex.create({}),
          graph.Vertex.create({}),
          graph.Vertex.create({}),
          graph.Vertex.create({}),
        ])
      })
      it('can retrieve all edges', async () => {
        const edges = await map(Bs, b => graph.edge.create({ subject: A, predicate: 'AHasB', object: b }))
        const traversedEdges = (await bluestream.collect(
          graph.edge.traverse({ predicate: 'AHasB' })
        )) as EdgeDefinition[]

        assert.equal(traversedEdges.length, edges.length)
        assert.containSubset(traversedEdges, edges)
      })
    })
    describe('subject and object', () => {
      let subject: GraphDBNode
      let object: GraphDBNode
      let predicates: string[]
      beforeEach(async () => {
        const { graph: setupGraph } = await setupImplicitGraphTests(redis)
        graph = setupGraph
        subject = await graph.Vertex.create({})
        object = await graph.Vertex.create({})
        predicates = ['AHasB', 'AHasC', 'AHasD']
      })
      it('can traverse all edges', async () => {
        const edges = await map(predicates, predicate => graph.edge.create({ subject, predicate, object }))
        const traversedEdges = (await bluestream.collect(graph.edge.traverse({ subject, object }))) as EdgeDefinition[]

        assert.equal(traversedEdges.length, edges.length)
        assert.containSubset(traversedEdges, edges)
      })
    })
    describe('subject and predicate', () => {
      let A: GraphDBNode
      let Bs: GraphDBNode[]
      beforeEach(async () => {
        const { a, bs, graph: setupGraph } = await setupImplicitGraphTests(redis)
        graph = setupGraph
        A = a
        Bs = bs
      })
      it('can traverse edges', async () => {
        const edges = await map(Bs, b => graph.edge.create({ subject: A, predicate: 'AHasB', object: b }))
        const traversedEdges = (await bluestream.collect(
          graph.edge.traverse({ subject: A, predicate: 'AHasB' })
        )) as EdgeDefinition[]
        assert.containSubset(traversedEdges, edges)
      })
    })
    describe('object and predicate', () => {
      let A: GraphDBNode
      let Bs: GraphDBNode[]
      beforeEach(async () => {
        const { a, bs, graph: setupGraph } = await setupImplicitGraphTests(redis)
        graph = setupGraph
        A = a
        Bs = bs
      })
      it('can traverse edges', async () => {
        const edges = await map(Bs, b => graph.edge.create({ object: A, predicate: 'BHasA', subject: b }))
        const traversedEdges = (await bluestream.collect(
          graph.edge.traverse({ object: A, predicate: 'BHasA' })
        )) as EdgeDefinition[]
        assert.containSubset(traversedEdges, edges)
      })
    })
  })

  describe('#traverseByNode', () => {
    beforeEach(async () => {
      const {
        passengers: [passenger1, passenger2, passenger3],
      } = context
      await bluebird.join(
        Edge.create({
          subject: passenger1,
          object: passenger2,
          predicate: HasCarPoolBuddy,
        }),
        Edge.create({
          subject: passenger3,
          object: passenger1,
          predicate: HasCarPoolBuddy,
        })
      )
    })

    it('returns all of a cars edges', done => {
      const { car } = context
      const stream = Edge.traverseByNode(car)
      let objectCount = 0
      stream.on('data', data => {
        assert.containSubset(data, { predicate: `${predicate}` })
        objectCount++
      })

      stream.on('end', () => {
        assert.equal(objectCount, 3)
        done()
      })
    })

    it('returns all of passengers edges', done => {
      const {
        passengers: [passenger],
      } = context
      const stream = Edge.traverseByNode(passenger)
      let objectCount = 0
      stream.on('data', data => {
        objectCount++
      })

      stream.on('end', () => {
        assert.equal(objectCount, 3)
        done()
      })
    })
  })

  describe('Hexastore Edges', () => {
    describe('#delete', () => {
      it('can delete edges between two nodes', async () => {
        const {
          passengers: [passenger],
          car,
        } = context
        assert.equal(await Edge.count({ predicate }), 3)
        assert.equal(await Edge.count({ object: passenger, predicate }), 1)
        assert.equal(await Edge.count({ subject: car, predicate }), 3)

        await Edge.delete({ subject: car, object: passenger, predicate })

        assert.equal(await Edge.count({ predicate }), 2)
        assert.equal(await Edge.count({ object: passenger, predicate }), 0)
        const passengers = await Edge.findNodes({
          subject: car,
          predicate,
        })
        const passengerExists = passengers.find(t => t.id === passenger.id)
        assert.equal(passengers.length, 2)
        assert.notOk(passengerExists)
      })
    })
  })

  describe('#findNodesWithWeight', () => {
    it('can find nodes ordered by weight', async () => {
      const {
        car,
        passengers: [passenger1, passenger2, passenger3],
      } = context
      const nodes = await Edge.findNodesWithWeight({
        subject: car,
        predicate,
      })

      assert.deepEqual(nodes.map(({ weight }) => weight), [10, 20, 30])
      assert.deepEqual(nodes.map(({ node: { id } }) => id), [passenger2, passenger3, passenger1].map(({ id }) => id))
    })
  })
  describe('#delete', () => {
    it('can delete weighted edges between two nodes', async () => {
      const {
        car,
        passengers: [passenger],
      } = context
      let nodes = await Edge.findNodesWithWeight({
        subject: car,
        predicate,
      })
      assert.equal(nodes.length, 3)

      const isDeleted = await Edge.delete({
        subject: car,
        object: passenger,
        predicate,
      })
      assert.equal(isDeleted, true)

      nodes = await Edge.findNodesWithWeight({
        subject: car,
        predicate,
      })
      const passengerExists = nodes.find(({ node: { id } }) => id === passenger.id)
      assert.equal(nodes.length, 2)
      assert.notOk(passengerExists)
    })
  })
  describe('#countIntersectedNodes', () => {
    beforeEach(async () => {
      const { passengers } = context
      const drivers = await Promise.all([
        graph.Passenger.create({ name: 'Ein' }),
        graph.Passenger.create({ name: 'Ein' }),
      ])
      context.drivers = drivers
      await Promise.all([
        Edge.create({
          subject: drivers[0],
          object: passengers[1],
          predicate: HasCarPoolBuddy,
          weight: 1,
        }),
        Edge.create({
          subject: drivers[0],
          object: passengers[2],
          predicate: HasCarPoolBuddy,
          weight: 2,
        }),
        Edge.create({
          subject: drivers[1],
          object: passengers[1],
          predicate: HasCarPoolBuddy,
          weight: 5,
        }),
        Edge.create({
          subject: drivers[1],
          object: passengers[2],
          predicate: HasCarPoolBuddy,
          weight: 3,
        }),
      ])
    })

    it('can find shared edges between two nodes', async () => {
      const {
        drivers: [driver1, driver2],
      } = context
      const predicatedNodes = [
        { subject: driver1, predicate: HasCarPoolBuddy },
        { subject: driver2, predicate: HasCarPoolBuddy },
      ]
      const count = await Edge.countIntersectedNodes({
        predicatedNodes,
      })
      assert.equal(count, 2)
    })
  })
  describe('#intersectedNodes', () => {
    beforeEach(async () => {
      const { passengers } = context
      const drivers = await bluebird.join(
        graph.Passenger.create({ name: 'Ein' }),
        graph.Passenger.create({ name: 'Millie' })
      )
      context.drivers = drivers
      await Promise.all([
        Edge.create({
          subject: drivers[0],
          object: passengers[0],
          predicate: HasCarPoolBuddy,
          weight: 1,
        }),
        Edge.create({
          subject: drivers[0],
          object: passengers[1],
          predicate: HasCarPoolBuddy,
          weight: 2,
        }),
        Edge.create({
          subject: drivers[1],
          object: passengers[0],
          predicate: HasCarPoolBuddy,
          weight: 5,
        }),
        Edge.create({
          subject: drivers[1],
          object: passengers[1],
          predicate: HasCarPoolBuddy,
          weight: 3,
        }),
      ])
    })

    it('can find shared edges between two nodes', async () => {
      const {
        drivers: [driver1, driver2],
        passengers: [passenger1, passenger2],
      } = context
      const predicatedNodes = [
        { subject: driver1, predicate: HasCarPoolBuddy },
        { subject: driver2, predicate: HasCarPoolBuddy },
      ]
      const sharedPassengers = (await Edge.intersectedNodes({
        order: 'DESC',
        aggregate: 'SUM',
        predicatedNodes,
      })) as Array<{ node: GraphDBNode }>

      assert.equal(sharedPassengers[0].node.id, passenger1.id)
      assert.equal(sharedPassengers[1].node.id, passenger2.id)
    })
  })

  describe('Data on the Edge', () => {
    let subject: GraphDBNode
    let object: GraphDBNode
    const predicate = RegistrationNumber
    beforeEach(async () => {
      const {
        car,
        passengers: [passenger],
      } = context
      subject = passenger
      object = car
      await graph.edge.create({ subject, object, predicate, fields: { registration: '1234' } })
    })
    it('Creates data on the edge on create', async () => {
      const { fields } = await graph.edge.create({ subject, object, predicate, fields: { registration: '1234' } })
      assert.containSubset(fields, { registration: '1234' })
      assert.isOk((await redis.keys('*e:d:*'))[0])
    })
    it('Finds data on the edge in find', async () => {
      const [edge] = await graph.edge.find({ subject, predicate })
      assert.containSubset(edge, { fields: { registration: '1234' } })
    })
    it('Deletes data from the edge on delete', async () => {
      assert.isOk((await redis.keys('*e:d:*'))[0])
      await graph.edge.delete({ subject, object, predicate })
      assert.isNotOk((await redis.keys('*e:d:*'))[0])
    })
    it('#findFields', async () => {
      assert.containSubset(await graph.edge.findFields({ subject, object, predicate }), { registration: '1234' })
    })
  })
})
