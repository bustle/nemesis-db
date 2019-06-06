import invariant from 'invariant'
import { map, join } from 'bluebird'
import crypto from 'crypto'
import { sortBy, intersectionBy } from 'lodash'
import { RedisLoader } from 'redis-loader'
import { writeToStream, concat } from 'streaming-iterables'
import { PassThrough } from 'stream'
import { invariantNode } from '../invariant'
import {
  createIdBuffer,
  readIdBuffer,
  isSubjectOf,
  isObjectOf,
  isObjectOfInvariant,
  isSubjectOfInvariant,
  subjectEdgeDefs,
  objectEdgeDefs,
} from './edge-utils'
import {
  find,
  STATES,
  findNodeType,
  IdObject,
  TypedIdObject,
  GUID,
  serializeFields,
  deserializeFields,
} from './node-utils'
import { ONE_TO_ONE, ONE_TO_MANY, MANY_TO_ONE } from './multiplicities'
import { GraphDBSchema, EdgeDefs } from './schema'
import { EdgeDefinition } from './definitions'
import { GraphDB } from 'graphdb/types'

const TEN_MINUTES_IN_SEC = 600

const ASC = 'ASC'
const DESC = 'DESC'
const EDGE_DATA_PREFIX = 'e:d'
const WEIGHTED_INDEX_PREFIX = 'e:w'
const INTERSECT_AGGREGATE_OPTIONS: ReadonlyArray<any> = ['MIN', 'MAX', 'SUM']

export type Predicate = string | EdgeDefinition

export interface WeightlessEdgeObject {
  readonly object: IdObject
  readonly predicate: Predicate
  readonly subject: IdObject
}

export interface GraphDBEdgeFields {
  [key: string]: any
}

export interface EdgeObject extends WeightlessEdgeObject {
  readonly weight: number
  readonly fields?: GraphDBEdgeFields | null
}

export interface CreateEdgeOptions extends WeightlessEdgeObject {
  readonly weight?: number
  readonly fields?: GraphDBEdgeFields | null
}

export interface FindNodesWithWeightOptions {
  readonly subject?: IdObject
  readonly object?: IdObject
  readonly predicate: Predicate
  /**
   * Filter by minimum weight
   */
  readonly min?: number
  /**
   * Filter by maxium weight
   */
  readonly max?: number
  readonly offset?: number
  readonly limit?: number
  readonly order?: 'ASC' | 'DESC'
}

async function getTypedIdObject(redis: RedisLoader, object: IdObject | TypedIdObject): Promise<TypedIdObject> {
  if ((object as TypedIdObject)._nodeType) {
    return object as TypedIdObject
  }

  return {
    ...object,
    _nodeType: await findNodeType(redis, object),
  }
}

export interface SubjectNode {
  readonly predicate: Predicate
  readonly subject: IdObject
}

export interface ObjectNode {
  readonly predicate: Predicate
  readonly object: IdObject
}

type PredicateNode =
  | {
      readonly predicate: Predicate
      readonly subject: IdObject
      readonly object?: IdObject
    }
  | {
      readonly predicate: Predicate
      readonly subject?: IdObject
      readonly object: IdObject
    }

export class GraphEdgeLoader {
  readonly redis: RedisLoader
  readonly schema: GraphDBSchema

  constructor({ redis, schema }: { readonly redis: RedisLoader; readonly schema: GraphDBSchema }) {
    invariant(redis, '"redis" is required')
    invariant(schema, '"schema" is required')
    this.schema = schema
    this.redis = redis
  }

  get edgeDefs() {
    return this.schema.edgeDefs
  }

  /**
   * Create an edge between two nodes
   */
  async create({ subject: inputSubject, object: inputObject, predicate, weight = 0, fields }: CreateEdgeOptions) {
    const { redis, edgeDefs } = this
    if (!inputSubject) {
      throw new Error('"subject" is a required argument')
    }
    if (!inputObject) {
      throw new Error('"object" is a required argument')
    }
    const subject = await getTypedIdObject(redis, inputSubject)
    const object = await getTypedIdObject(redis, inputObject)

    invariantNode(object)
    invariantNode(subject)
    invariant(predicate, `Predicate is required`)
    const edgeDef = edgeDefs[`${predicate}`]
    invariant(edgeDef, `Unknown predicate "${predicate}". All predicates must be included in graphdb/edges.js`)
    isObjectOfInvariant(edgeDef, object)
    isSubjectOfInvariant(edgeDef, subject)
    const edgeExists = await this.exists({ subject, object, predicate })
    if (!edgeExists) {
      if (ONE_TO_ONE === edgeDef.multiplicity || MANY_TO_ONE === edgeDef.multiplicity) {
        invariant(
          !(await this.count({ subject, predicate })),
          `"subject" with id: ${subject.id} already has an edge with "predicate": ${edgeDef.name}`
        )
      }
      if (ONE_TO_ONE === edgeDef.multiplicity || ONE_TO_MANY === edgeDef.multiplicity) {
        invariant(
          !(await this.count({ object, predicate })),
          `"object" with id: ${object.id} already has an edge with "predicate": ${edgeDef.name}`
        )
      }
    }

    const statuses: any[] = await Promise.all([
      redis.exists(`node:fields:${subject.id}`),
      redis.hget(`node:fields:${subject.id}`, '_state'),
      redis.exists(`node:fields:${object.id}`),
      redis.hget(`node:fields:${object.id}`, '_state'),
    ])
    if (statuses[0] === 0 || statuses[1] === STATES.DELETING) {
      throw new Error(`Subject node with id ${subject.id} does not exist`)
    }
    if (statuses[2] === 0 || statuses[3] === STATES.DELETING) {
      throw new Error(`Object node with id ${object.id} does not exist`)
    }

    const subjectKey = createWeightedKey({ subject, predicate })
    const objectKey = createWeightedKey({ object, predicate })
    const predicateKey = createWeightedKey({ predicate })
    const dataOnEdgeKey = createDataOnEdgeKey({ subject, object, predicate })
    const serializedFields = fields && serializeFields(edgeDef, fields)

    const objectBuff = createIdBuffer(object)
    const subjectBuff = createIdBuffer(subject)

    await Promise.all([
      redis.zaddBuffer(objectKey, String(weight), subjectBuff),
      redis.zaddBuffer(subjectKey, String(weight), objectBuff),
      redis.zaddBuffer(predicateKey, String(weight), Buffer.concat([subjectBuff, objectBuff])),
      serializedFields && redis.hmset(dataOnEdgeKey, serializedFields),
    ])

    return {
      subject: { id: subject.id },
      object: { id: object.id },
      predicate: `${predicate}`,
      weight,
      fields,
    }
  }

  /**
   * Count edges
   */
  async count({
    subject,
    object,
    predicate,
    min = -Infinity,
    max = Infinity,
  }: {
    readonly max?: number
    readonly min?: number
    readonly object?: IdObject
    readonly predicate?: Predicate
    readonly subject?: IdObject
  } = {}): Promise<number> {
    const { redis, edgeDefs } = this
    if (predicate && (subject || object)) {
      return this._countWeightedEdges({
        subject,
        object,
        predicate,
        min,
        max,
      })
    } else if (subject && object) {
      // find all the potential predicates a given subject can have
      const potentialPredicates = await sortedPotentialPredicates({ edgeDefs, redis, subject, object })

      const objectBuff = createIdBuffer(object)
      return (await map(potentialPredicates, async predicate => {
        const subjectKey = createWeightedKey({ subject, predicate })
        // if the object id is in the set return one
        return (await redis.zscoreBuffer(subjectKey, objectBuff)) !== null ? 1 : 0
      })).reduce((acc, num) => acc + num, 0)
    } else if (subject || object) {
      // find all the potential predicates a given node can have
      const potentialPredicates = await sortedPotentialPredicates({ edgeDefs, redis, subject, object })
      // find a count of how many edges each one has
      return (await map(potentialPredicates, predicate =>
        this._countWeightedEdges({
          subject,
          object,
          predicate,
          min,
          max,
        })
      )).reduce((acc, num) => acc + num, 0)
    } else if (predicate) {
      return this._countWeightedEdges({ predicate, min, max })
    } else {
      return (await map(Object.values(edgeDefs), predicate =>
        this._countWeightedEdges({ predicate, min, max })
      )).reduce((acc, num) => acc + num, 0)
    }
  }

  async _countWeightedEdges({
    subject,
    object,
    predicate,
    min = -Infinity,
    max = Infinity,
  }: {
    readonly predicate: Predicate
    readonly subject?: IdObject
    readonly object?: IdObject
    readonly min: number
    readonly max: number
  }): Promise<number> {
    const key = createWeightedKey({
      subject,
      predicate,
      object,
    } as PredicateNode)

    if (min === -Infinity && max === Infinity) {
      return this.redis.zcard(key)
    }
    return this.redis.zcount(key, min, max)
  }

  /**
   * Find edges
   */
  async find({
    subject,
    object,
    predicate,
    min = -Infinity,
    max = Infinity,
    offset = 0,
    limit = 10,
    /**
     * Order is within each predicate
     */
    order = ASC,
  }: {
    readonly subject?: IdObject
    readonly object?: IdObject
    readonly predicate?: Predicate
    readonly min?: number
    readonly max?: number
    readonly offset?: number
    readonly limit?: number
    readonly order?: 'ASC' | 'DESC'
  }): Promise<EdgeObject[]> {
    const { redis, edgeDefs } = this

    let edges: EdgeObject[] = []
    invariant([ASC, DESC].includes(order), `"order" value must be either ${ASC} or ${DESC}`)
    if (predicate && (subject || object)) {
      edges = await this._findWeightedEdges({ subject, object, predicate, offset, limit, order, min, max })
    } else if (subject && object) {
      // find all the potential predicates a given node can have
      const potentialPredicates = await sortedPotentialPredicates({ edgeDefs, redis, subject, object })
      let currentOffset = offset
      const objectBuff = createIdBuffer(object)
      for (const predicate of potentialPredicates) {
        const subjectKey = createWeightedKey({ subject, predicate })
        const weight = await redis.zscoreBuffer(subjectKey, objectBuff)
        if (weight === null) {
          continue
        }
        // skip if node and predicate have no edges or if offset is more than count
        if (currentOffset > 0) {
          currentOffset--
          continue
        }
        edges.push({
          subject,
          object,
          predicate: `${predicate}`,
          weight: Number(weight),
        })
        // end if we've found the amount of edges we need
        if (edges.length === limit) {
          break
        }
      }
    } else if (!predicate && (subject || object)) {
      const node = subject ? { subject } : { object }
      // find all the potential predicates a given node can have
      const potentialPredicates = await sortedPotentialPredicates({ edgeDefs, redis, subject, object })
      // find a count of how many edges each one has
      const counts = await map(potentialPredicates, predicate => this.count({ ...node, predicate, min, max }))
      // array to put all the found edges in
      const foundEdges: EdgeObject[] = []
      let currentOffset = offset
      for (let i = 0; i < counts.length; i++) {
        const count = counts[i]
        const predicate = potentialPredicates[i]
        // skip if node and predicate have no edges or if offset is more than count
        if (count === 0 || currentOffset >= count) {
          currentOffset -= count
          continue
        }
        // calc the number of edges we need to satisfy
        const currentLimit = limit - foundEdges.length
        // find edges for given predicate and subject
        // tslint:disable-next-line: prettier
        const edgeBatch = await this._findWeightedEdges({ ...node, predicate, min, max, offset: currentOffset, limit: currentLimit, order })

        // our offset is now exhausted
        if (currentOffset > 0) {
          currentOffset = 0
        }

        foundEdges.push(...edgeBatch)
        // end if we've found the amount of edges we need
        if (foundEdges.length === limit) {
          break
        }
        // if we don't have enough edges move on to the next
      }
      edges = foundEdges
    } else if (predicate) {
      edges = await this._findWeightedEdges({ predicate, offset, limit, order, min, max })
    } else {
      throw new Error(`Must provide at least a 'subject', 'predicate', or 'object'`)
    }
    return map(edges, async (edge: EdgeObject) => {
      const fields = await this.findFields(edge)
      return { ...edge, fields }
    })
  }

  async _findWeightedEdges({
    subject,
    object,
    predicate,
    min,
    max,
    offset,
    limit,
    order,
  }: {
    readonly subject?: IdObject
    readonly object?: IdObject
    readonly predicate: Predicate
    readonly min: number
    readonly max: number
    readonly offset: number
    readonly limit: number
    readonly order: 'ASC' | 'DESC'
  }) {
    const key = createWeightedKey({
      subject,
      predicate,
      object,
    } as PredicateNode)
    let weightedIds: Buffer[]
    if (order === ASC) {
      weightedIds = await this.redis.zrangebyscoreBuffer(
        key,
        min,
        max,
        'WITHSCORES',
        'LIMIT',
        String(offset),
        String(limit)
      )
    } else {
      weightedIds = await this.redis.zrevrangebyscoreBuffer(
        key,
        max,
        min,
        'WITHSCORES',
        'LIMIT',
        String(offset),
        String(limit)
      )
    }

    return createEdgesFromWeightedIds({
      weightedIds,
      subject,
      object,
      predicate,
    })
  }
  /**
   * return a matching edge's fields or an empty object if no edge or data exists with provided information
   */
  async findFields({ subject, predicate, object }: WeightlessEdgeObject) {
    invariant(subject, 'must have a subject')
    invariant(predicate, 'must have a predicate')
    invariant(object, 'must have a object')
    const edgeDef = this.edgeDefs[`${predicate}`]
    if (edgeDef.fields) {
      const dataOnEdgeKey = createDataOnEdgeKey({ subject, object, predicate })
      const serializedFields = await this.redis.hgetall(dataOnEdgeKey)
      return deserializeFields(edgeDef, serializedFields)
    }
    return undefined
  }

  /**
   * Return an iterator of all matching edges
   *
   * @param {object} options the options hash
   * @param {node} [options.subject =] the subject node (subject) (use for weight)
   * @param {node} [options.object =] the to node (object)
   * @param {string} [options.predicate =] the kind of edge (predicate)
   * @param {integer} [options.limit = 500] max number of edges per batch
   * @returns {array} returns a stream of edges
   */
  async *traverseItr({
    subject,
    predicate,
    object,
    limit = 500,
  }: {
    readonly subject?: IdObject
    readonly object?: IdObject
    readonly predicate?: Predicate
    readonly limit?: number
  }) {
    const { redis, edgeDefs } = this
    if (predicate && subject && object) {
    } else if (predicate && (subject || object)) {
      const key = createWeightedKey({ subject, predicate, object })
      for await (const weightedIds of redis.zscanBufferIterable(key, { count: limit })) {
        yield* createEdgesFromWeightedIds({ weightedIds, predicate, subject, object })
      }
    } else if (subject && object) {
      // find all the potential predicates a given node can have
      const potentialPredicates = await sortedPotentialPredicates({ subject, object, edgeDefs, redis })
      const weights = await map(potentialPredicates, async predicate => {
        const key = createWeightedKey({ subject, predicate })
        return this.redis.zscoreBuffer(key, createIdBuffer(object))
      })
      for (let index = 0; index < weights.length; index++) {
        if (weights[index] === null) {
          continue
        }
        const weight = Number(weights[index])
        const predicate = potentialPredicates[index]
        const fields = await this.findFields({ subject, predicate, object })
        yield { subject, predicate: predicate.toString(), object, weight, fields }
      }
    } else if (subject || object) {
      const potentialPredicates = await sortedPotentialPredicates({ subject, object, edgeDefs, redis })
      const counts = await map(potentialPredicates, predicate => this.count({ object, subject, predicate }))
      for (let i = 0; i < counts.length; i++) {
        const count = counts[i]
        const predicate = potentialPredicates[i]
        if (!count) {
          continue
        }
        const key = createWeightedKey({ subject, predicate, object })
        for await (const weightedIds of redis.zscanBufferIterable(key, { count: limit })) {
          yield* createEdgesFromWeightedIds({ weightedIds, predicate, subject, object })
        }
      }
    } else if (predicate) {
      const key = createWeightedKey({ predicate })
      for await (const weightedIds of redis.zscanBufferIterable(key, { count: limit })) {
        yield* createEdgesFromWeightedIds({ weightedIds, predicate, subject, object })
      }
    } else {
      const predicates = sortBy(Object.values(edgeDefs), 'name')
      const counts = await map(predicates, predicate => this.count({ predicate }))
      for (let i = 0; i < counts.length; i++) {
        const count = counts[i]
        const predicate = predicates[i]
        if (!count) {
          continue
        }
        const key = createWeightedKey({ predicate })
        for await (const weightedIds of redis.zscanBufferIterable(key, { count: limit })) {
          yield* createEdgesFromWeightedIds({ weightedIds, predicate })
        }
      }
    }
  }

  /**
   * Return a stream of all matching edges
   *
   * @param {object} options the options hash
   * @param {node} [options.subject =] the subject node (subject) (use for weight)
   * @param {node} [options.object =] the to node (object)
   * @param {string} [options.predicate =] the kind of edge (predicate)
   * @param {integer} [options.limit = 2000] max number of edges per batch
   * @returns {array} returns a stream of edges
   */
  traverse(
    input: {
      readonly subject?: IdObject
      readonly object?: IdObject
      readonly predicate?: Predicate
      readonly limit?: number
    } = {}
  ) {
    const stream = new PassThrough({ objectMode: true })

    writeToStream(stream, this.traverseItr(input)).then(() => stream.end(), err => stream.emit(err))
    return stream
  }

  /**
   * traverse hexastore edges by node.
   *
   * @param {node} node the node to look for all edges for
   * @returns {stream} returns a stream of edges (note that because of the nature of the hexastore you can be returned duplicate edges with this method)
   */
  traverseByNode(node: IdObject) {
    invariant(node.id, `"node.id" must be present`)
    const stream = new PassThrough({ objectMode: true })
    const iterator = concat(this.traverseItr({ subject: node }), this.traverseItr({ object: node }))

    writeToStream(stream, iterator).then(() => stream.end(), err => stream.emit(err))
    return stream
  }

  /**
   * Find the nodes connected to a node
   */
  async findNodes<T extends GraphDB.Node>({
    subject,
    object,
    predicate,
    offset = 0,
    limit = 10,
    order = ASC,
    min = -Infinity,
    max = Infinity,
  }: FindNodesWithWeightOptions) {
    if (!subject && !object) {
      throw new Error('Field `subject` or `object` is required')
    }
    if (subject && object) {
      throw new Error('Cannot provide both `subject` or `object` fields')
    }

    const edges = await this.find({
      subject,
      object,
      predicate,
      offset,
      limit,
      order,
      max,
      min,
    })

    const ids = edges.map(edge => extractNodeId({ node: subject ? 'object' : 'subject', edge }))

    return map(ids, async id => {
      const node = await find<T>(this.redis, this.schema, id)
      if (!node) {
        throw new Error(
          `findNodes(${JSON.stringify({ subject, predicate, object })}) tried to find Node:${id} but couldn't.`
        )
      }
      return node
    })
  }

  /**
   * Find the weighted nodes connected to a node
   */
  async findNodesWithWeight({
    subject,
    object,
    predicate,
    min = -Infinity,
    max = Infinity,
    offset = 0,
    limit = 10,
    order = ASC,
  }: FindNodesWithWeightOptions) {
    const edges = await this.find({
      subject,
      object,
      predicate,
      min,
      max,
      offset,
      limit,
      order,
    })

    const nodes = await map(edges, async edge => {
      const id = extractNodeId({ node: subject ? 'object' : 'subject', edge })
      const node = await find(this.redis, this.schema, id)
      if (!node) {
        throw new Error(`findNodesWithWeight: Unable to load node:${id} from edge ${JSON.stringify(edge)}`)
      }
      return node
    })

    return nodes.map((node, i) => ({ weight: edges[i].weight, node }))
  }

  /**
   * Deletes the edge between two nodes
   */
  async delete({
    subject,
    object,
    predicate,
  }: {
    readonly subject: IdObject
    readonly object: IdObject
    readonly predicate: Predicate
  }): Promise<boolean> {
    const { edgeDefs, redis } = this
    invariant(object.id, `Expected the 'object' object to have attribute "id"`)
    invariant(subject.id, `Expected the 'subject' object to have attribute "id"`)
    invariant(
      edgeDefs[predicate.toString()],
      `Unknown predicate "${predicate}". All predicates must be included in models/edges.js`
    )

    const objectBuff = createIdBuffer(object)
    const subjectBuff = createIdBuffer(subject)

    const subjectKey = createWeightedKey({ subject, predicate })
    const objectKey = createWeightedKey({ object, predicate })
    const predicateKey = createWeightedKey({ predicate })
    const dataOnEdgeKey = createDataOnEdgeKey({ subject, object, predicate })

    await Promise.all([
      redis.zremBuffer(subjectKey, objectBuff),
      redis.zremBuffer(objectKey, subjectBuff),
      redis.zremBuffer(predicateKey, Buffer.concat([subjectBuff, objectBuff])),
      redis.del(dataOnEdgeKey),
    ])

    return true
  }

  async deleteAllForNode({ id }: IdObject) {
    invariant(id, 'deleteAll: "id" is required')
    // loop through all subject edges and delete them
    while (await this.count({ subject: { id } })) {
      const edges = await this.find({ subject: { id }, limit: 1000 })
      await map(edges, e => this.delete(e))
    }
    // loop through all object edges and delete them
    while (await this.count({ object: { id } })) {
      const edges = await this.find({ object: { id }, limit: 1000 })
      await map(edges, e => this.delete(e))
    }
  }

  /**
   * Returns if an edge exists
   *
   * @param {object} options the options hash
   * @param {node} options.subject the subject node (subject)
   * @param {node} options.object the to node (object)
   * @param {string} options.predicate the kind of edge (predicate)
   * @returns {boolean} true if the edge exists false if it doesn't
   */
  async exists({ subject, object, predicate }: WeightlessEdgeObject) {
    const { edgeDefs, redis } = this
    invariant(subject, `"subject" must be in the edge definition`)
    invariant(object, `"object" must be in the edge definition`)
    invariant(
      edgeDefs[predicate.toString()],
      `Unknown predicate "${predicate}". All predicates must be included in models/edges.js`
    )
    const objectBuff = createIdBuffer(object)
    const key = createWeightedKey({ subject, predicate })

    const score = await redis.zscoreBuffer(key, objectBuff)
    return score !== null
  }

  /**
   * Count intersected edges
   */
  async countIntersectedNodes({
    min = -Infinity,
    max = Infinity,
    aggregate = 'MAX',
    predicatedNodes,
  }: {
    readonly min?: number
    readonly max?: number
    readonly aggregate?: 'SUM' | 'MAX' | 'MIN'
    readonly predicatedNodes: ReadonlyArray<PredicateNode>
  }) {
    invariant(predicatedNodes.length > 1, 'must provide two or more predicated nodes for intersection')
    const { redis } = this
    const edgeKeys = predicatedNodes.map(createWeightedKey)
    const interstoreKey = createInterstoreKey(edgeKeys, aggregate)
    const ttl = await redis.ttl(interstoreKey)

    if (ttl < 30) {
      await join(
        redis.zinterstore(interstoreKey, edgeKeys.length, ...edgeKeys, 'AGGREGATE', aggregate),
        redis.expire(interstoreKey, TEN_MINUTES_IN_SEC)
      )
    }

    if (min === -Infinity && max === Infinity) {
      return redis.zcard(interstoreKey)
    }
    return redis.zcount(interstoreKey, min, max)
  }

  /**
   * Search intersected edges
   */
  async intersectedNodes({
    min = -Infinity,
    max = Infinity,
    offset = 0,
    limit = 100,
    order = ASC,
    aggregate = 'MAX',
    predicatedNodes,
  }: {
    readonly min?: number
    readonly max?: number
    readonly offset?: number
    readonly limit?: number
    readonly order?: 'ASC' | 'DESC'
    readonly aggregate?: 'SUM' | 'MAX' | 'MIN'
    readonly predicatedNodes: ReadonlyArray<PredicateNode>
  }) {
    invariant(predicatedNodes.length > 1, 'must provide two or more predicated nodes for intersection')
    invariant(
      INTERSECT_AGGREGATE_OPTIONS.includes(aggregate),
      `"aggregate" value must be one of the following ${INTERSECT_AGGREGATE_OPTIONS.join(', ')}`
    )
    const { redis, schema } = this
    const edgeKeys = predicatedNodes.map(createWeightedKey)
    const interstoreKey = createInterstoreKey(edgeKeys, aggregate)
    const ttl = await redis.ttl(interstoreKey)

    if (ttl < 30) {
      await join(
        redis.zinterstore(interstoreKey, edgeKeys.length, ...edgeKeys, 'AGGREGATE', aggregate),
        redis.expire(interstoreKey, TEN_MINUTES_IN_SEC)
      )
    }

    let idsAndWeights
    if (order === ASC) {
      idsAndWeights = await redis.zrangebyscoreBuffer(interstoreKey, min, max, 'WITHSCORES', 'LIMIT', offset, limit)
    } else {
      idsAndWeights = await redis.zrevrangebyscoreBuffer(interstoreKey, max, min, 'WITHSCORES', 'LIMIT', offset, limit)
    }
    const nodeIds: GUID[] = []
    const weights: number[] = []
    for (let i = 0; i < idsAndWeights.length; i += 2) {
      nodeIds.push(readIdBuffer(idsAndWeights[i]))
      weights.push(Number(idsAndWeights[i + 1]))
    }

    const nodes = await map(nodeIds, id => find(redis, schema, id))
    const results = nodes.map((node, i) => ({ node, weight: weights[i] }))

    return results
  }

  /**
   * Check if subject is valid for edge definition
   */
  async isValidSubject({ subject: inputSubject, predicate }: SubjectNode) {
    const { redis, edgeDefs } = this
    if (!inputSubject) {
      throw new Error('"subject" is a required argument')
    }
    const subject = await getTypedIdObject(redis, inputSubject)
    invariantNode(subject)
    invariant(predicate, `Predicate is required`)
    const edgeDef = edgeDefs[predicate.toString()]
    invariant(edgeDef, `Unknown predicate "${predicate}". All predicates must be included in graphdb/edges.js`)

    return isSubjectOf(edgeDef, subject)
  }

  /**
   * Check if object is valid for edge definition
   */
  async isValidObject({ object: inputObject, predicate }: ObjectNode) {
    const { redis, edgeDefs } = this
    if (!inputObject) {
      throw new Error('"object" is a required argument')
    }
    const object = await getTypedIdObject(redis, inputObject)
    invariantNode(object)
    invariant(predicate, `Predicate is required`)
    const edgeDef = edgeDefs[predicate.toString()]
    invariant(edgeDef, `Unknown predicate "${predicate}". All predicates must be included in graphdb/edges.js`)

    return isObjectOf(edgeDef, object)
  }
}

function extractNodeId({ node, edge }: { readonly node: 'subject' | 'object'; readonly edge: EdgeObject }) {
  return node === 'object' ? edge.object.id : edge.subject.id
}

function createInterstoreKey(keys: ReadonlyArray<string>, aggregate: string) {
  const jumboKey = `${aggregate}:${[...keys].sort().join()}`
  return crypto
    .createHash('sha256')
    .update(jumboKey, 'utf8')
    .digest('hex')
}

interface CreateWeightedKeyOptions {
  readonly predicate: Predicate
  readonly subject?: IdObject
  readonly object?: IdObject
}
function createWeightedKey({ subject, object, predicate }: CreateWeightedKeyOptions) {
  if (subject) {
    return `${WEIGHTED_INDEX_PREFIX}:s:${subject.id}:${predicate}`
  }
  if (object) {
    return `${WEIGHTED_INDEX_PREFIX}:o:${object.id}:${predicate}`
  }
  return `${WEIGHTED_INDEX_PREFIX}:p:${predicate}`
}

function createEdgesFromWeightedIds({
  weightedIds,
  subject,
  object,
  predicate,
}: {
  readonly weightedIds: ReadonlyArray<Buffer>
  readonly predicate: Predicate
  readonly subject?: IdObject
  readonly object?: IdObject
}) {
  const edges: EdgeObject[] = []
  for (let i = 0; i < weightedIds.length; i += 2) {
    const member = weightedIds[i]
    const weight = Number(weightedIds[i + 1])

    if (subject) {
      edges.push({
        subject: { id: subject.id },
        object: { id: readIdBuffer(member) },
        predicate: `${predicate}`,
        weight,
      })
    } else if (object) {
      edges.push({
        subject: { id: readIdBuffer(member) },
        object: { id: object.id },
        predicate: `${predicate}`,
        weight,
      })
    } else {
      const [subjectId, objectId] = [readIdBuffer(member.slice(0, 4)), readIdBuffer(member.slice(4, 8))]
      edges.push({
        subject: { id: subjectId },
        object: { id: objectId },
        predicate: `${predicate}`,
        weight,
      })
    }
  }
  return edges
}

function createDataOnEdgeKey({ subject, object, predicate }: WeightlessEdgeObject) {
  return `${EDGE_DATA_PREFIX}:${predicate}:${subject.id}:${object.id}`
}

async function sortedPotentialPredicates({
  subject,
  object,
  redis,
  edgeDefs,
}: {
  subject?: IdObject
  object?: IdObject
  redis: RedisLoader
  edgeDefs: EdgeDefs
}) {
  if (!subject && !object) {
    throw new Error('Subject or object is required')
  } else if (subject && object) {
    const subjectPredicates = subjectEdgeDefs(edgeDefs, await getTypedIdObject(redis, subject))
    const objectPredicates = objectEdgeDefs(edgeDefs, await getTypedIdObject(redis, object))
    return sortBy(intersectionBy(subjectPredicates, objectPredicates, 'name'), 'name')
  } else if (subject) {
    return sortBy(subjectEdgeDefs(edgeDefs, await getTypedIdObject(redis, subject)), 'name')
  } else if (object) {
    return sortBy(objectEdgeDefs(edgeDefs, await getTypedIdObject(redis, object)), 'name')
  } else {
    throw new Error('Idk how this could happen but apparently ts thinks its possible')
  }
}
