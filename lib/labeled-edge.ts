import { map } from 'bluebird'
import invariant from 'invariant'
import { transform } from 'bluestream'
import { IdObject } from './node-utils'
import { Predicate } from './edge'
import { RedisLoader } from 'redis-loader/dist'
import { GraphDBSchema } from './schema'

const KEY_PREFIX = 'e:l'
const OBJECT_EDGE_LABEL = '_o_e_'
const SUBJECT_EDGE_LABEL = '_s_e_'

const createLabeledEdgeKey = ({ id }: IdObject, predicate: string) => `${KEY_PREFIX}:${id}:${predicate}`
const createObjectStoreKey = ({ id }: IdObject) => `${KEY_PREFIX}:${OBJECT_EDGE_LABEL}:${id}`
const createSubjectStoreKey = ({ id }: IdObject) => `${KEY_PREFIX}:${SUBJECT_EDGE_LABEL}:${id}`

export interface LabeledEdgeSearch {
  readonly predicate: Predicate
  readonly subject: IdObject
  readonly value: string
}
/**
 * A unique labeled edge between a subject and value to an object.
 * For example `{ subject: Site, predicate: SiteHasContentByPath, value: '/p/foobar', object: Post }`
 */
export interface LabeledEdge extends LabeledEdgeSearch {
  readonly object: IdObject
}

interface CountValuesOptions {
  predicate: Predicate
  subject: IdObject
  object: IdObject
}
interface FindValuesOptions extends CountValuesOptions {
  limit?: number
  offset?: number
}

export class LabeledEdgeLoader {
  get edgeDefs() {
    return this.schema.edgeDefs
  }
  readonly redis: RedisLoader
  readonly schema: GraphDBSchema
  constructor({ schema, redis }: { redis: RedisLoader; schema: GraphDBSchema }) {
    invariant(schema, '"schema" is required')
    invariant(redis, '"redis" is required')
    this.schema = schema
    this.redis = redis
  }

  /**
   * traverse all the indexed values of a subject and predicate in batches
   *
   * @param {object} options the options hash
   * @param {node} options.subject the subject node (subject)
   * @param {string} options.predicate the kind of edge (predicate)
   * @param {number} [options.limit] the batch size
   * @returns {stream} a write stream that returns batches of LabeledEdge objects: [{ subject, predicate, object, value }, ...]
   */
  batchTraverse({ subject, predicate, limit = 1000 }: { limit?: number; predicate: Predicate; subject: IdObject }) {
    const key = createLabeledEdgeKey(subject, predicate.toString())
    const transformStream = transform(function(idsAndValues) {
      const objectsAndValues = []
      for (let i = 0; i < idsAndValues.length; i += 2) {
        const value = idsAndValues[i]
        const id = String(idsAndValues[i + 1])
        objectsAndValues.push({ subject, predicate, object: { id }, value })
      }
      // zscan doesnt always return the desired count https://github.com/antirez/redis/issues/1723#issuecomment-42385837
      if (objectsAndValues.length > limit) {
        for (let i = 0; i < objectsAndValues.length; i += limit) {
          this.push(objectsAndValues.slice(i, i + limit))
        }
      } else {
        this.push(objectsAndValues)
      }
    })

    return this.redis.zscanStream(key, { count: limit } as any).pipe(transformStream)
  }
  /**
   * returns a count of the number of values a subject with a predicate has
   *
   * @param {object} options the options hash
   * @param {node} options.subject the subject node (subject)
   * @param {string} options.predicate the kind of edge (predicate)
   * @returns {number}
   */
  async count({ subject, predicate }: { predicate: Predicate; subject: IdObject }) {
    return this.redis.zcard(createLabeledEdgeKey(subject, predicate.toString()))
  }
  /**
   * returns a count of the number of values an object has with a subject and a predicate
   *
   * @param {object} options the options hash
   * @param {node} options.subject the subject node (subject)
   * @param {string} options.predicate the kind of edge (predicate)
   * @param {node} options.object the object node (object)
   * @returns {number}
   */
  async countValues({ subject, predicate, object }: CountValuesOptions) {
    return this.redis.zcount(
      createLabeledEdgeKey(subject, predicate.toString()),
      object.id,
      `(${Number(object.id) + 1}`
    )
  }
  /**
   * Create a unique labeled edge between two nodes via it's value
   *
   * @param {object} options the options hash
   * @param {node} options.subject the subject node (subject)
   * @param {node} options.object the to node (object)
   * @param {string} options.predicate the kind of edge (predicate)
   * @param {string|number} options.value the value use to look up the connection between two nodes
   * @returns {boolean} returns true if the labeled edge is created
   */
  async create({ subject, predicate, object, value }: LabeledEdge) {
    const { redis, edgeDefs } = this
    invariant(subject, 'field "subject" is required')
    invariant(predicate, 'field "predicate" is required')
    invariant(object, 'field "object" is required')
    invariant(value, 'field "value" is required')
    invariant(
      edgeDefs[predicate.toString()],
      `Unknown predicate "${predicate}". All predicates must be included in models/edges.js`
    )

    const labeledEdgeKey = createLabeledEdgeKey(subject, predicate.toString())
    const objectStoreKey = createObjectStoreKey(object)
    const subjectStoreKey = createSubjectStoreKey(subject)
    const score = await redis.zscore(labeledEdgeKey, value)

    if (score !== object.id) {
      invariant(
        !score,
        `the value ${value} is already in use for subject with id: ${subject.id} and predicate: ${predicate.toString()}`
      )
    }

    if (score === object.id) {
      return true
    } else {
      const score = String(object.id)
      await Promise.all([
        redis.zadd(labeledEdgeKey, score, value),
        redis.zadd(objectStoreKey, score, labeledEdgeKey),
        redis.zadd(subjectStoreKey, score, labeledEdgeKey),
      ])
      return true
    }
  }
  /**
   * delete an labeled edge for a subject given a predicate and value
   *
   * @param {object} options the options hash
   * @param {node} options.subject the subject node (subject)
   * @param {string} options.predicate the kind of edge (predicate)
   * @param {string|number} options.value the value use to look up the connection between two nodes
   * @returns {object} a pojo representing the labeled edge { object: { id } }
   */
  async delete({ subject, predicate, value }: LabeledEdgeSearch) {
    invariant(subject, 'field "subject" is required')
    invariant(predicate, 'field "predicate" is required')
    invariant(value, 'field "value" is required')

    const { redis } = this
    const labeledEdgeKey = createLabeledEdgeKey(subject, predicate.toString())
    const [id] = await Promise.all([redis.zscore(labeledEdgeKey, value), redis.zrem(labeledEdgeKey, value)])

    const objectStoreKey = createObjectStoreKey({ id })
    const objectMember = createLabeledEdgeKey(subject, predicate.toString())
    await redis.zrem(objectStoreKey, objectMember)

    return { object: { id: String(id) } }
  }
  /**
   * see if an labeled edge for a subject given a predicate and value exists
   *
   * @param {object} options the options hash
   * @param {node} options.subject the subject node (subject)
   * @param {string} options.predicate the kind of edge (predicate)
   * @param {string|number} options.value the value use to look up the connection between two nodes
   * @returns {boolean} returns true if the labeled edge exists or false if it does not
   */
  async exists({ subject, predicate, value, object }: LabeledEdge) {
    const { redis, edgeDefs } = this
    invariant(subject, 'field "subject" is required')
    invariant(predicate, 'field "predicate" is required')
    invariant(object, 'field "object" is required')
    invariant(value, 'field "value" is required')
    invariant(
      edgeDefs[predicate.toString()],
      `Unknown predicate "${predicate}". All predicates must be included in models/edges.js`
    )
    const key = createLabeledEdgeKey(subject, predicate.toString())
    const objectId = await redis.zscore(key, value)

    return String(objectId) === String(object.id)
  }
  /**
   * checks if a labeled edge for a subject given a predicate and value is in use
   *
   * @param {object} options the options hash
   * @param {node} options.subject the subject node (subject)
   * @param {string} options.predicate the kind of edge (predicate)
   * @param {string|number} options.value the value used to look up the connection between two nodes
   * @returns {boolean} returns true if the labeled edge is in use or false if it does not
   */
  async inUse({ subject, predicate, value }: LabeledEdgeSearch) {
    invariant(subject, 'field "subject" is required')
    invariant(predicate, 'field "predicate" is required')
    invariant(value, 'field "value" is required')

    const key = createLabeledEdgeKey(subject, predicate.toString())
    return Boolean(await this.redis.zscore(key, value))
  }
  /**
   * find an object indexed on another node by a value
   *
   * @returns a pojo representing the labeled edge { object: { id } }
   */
  async find({ subject, predicate, value }: LabeledEdgeSearch): Promise<LabeledEdge | null> {
    invariant(subject, 'field "subject" is required')
    invariant(predicate, 'field "predicate" is required')
    invariant(value, 'field "value" is required')

    const key = createLabeledEdgeKey(subject, predicate.toString())
    const id = await this.redis.zscore(key, value)
    if (id) {
      return {
        subject,
        predicate,
        value,
        object: { id: String(id) },
      }
    }
    return null
  }
  /**
   * find the values indexed between another node
   *
   * @returns a pojo representing the labeled edge values
   */
  async findValues({ subject, predicate, object, limit = 10, offset = 0 }: FindValuesOptions): Promise<string[]> {
    invariant(subject, 'field "subject" is required')
    invariant(predicate, 'field "predicate" is required')
    invariant(object, 'field "object" is required')

    const key = createLabeledEdgeKey(subject, predicate.toString())
    return this.redis.zrangebyscore(key, object.id, `(${Number(object.id) + 1}`, 'LIMIT', offset, limit)
  }
  /**
   * traverse all the indexed values of a subject and predicate one at a time
   *
   * @param {object} options the options hash
   * @param {node} options.subject the subject node (subject)
   * @param {string} options.predicate the kind of edge (predicate)
   * @param {number} [options.limit] the batch size
   * @returns {stream} a write stream that returns LabeledEdge objects : { subject, predicate, object, value }
   */
  traverse(input: { limit?: number; predicate: Predicate; subject: IdObject }) {
    const batchStream = this.batchTraverse(input)
    const traverseStream = transform(async function(edges) {
      edges.forEach((edge: LabeledEdge) => this.push(edge))
    })

    return batchStream.pipe(traverseStream)
  }

  /**
   * remove an object from all it's subjects' labeled edge stores
   *
   * @param {object} options the options hash
   * @param {node} options.object the object node (object)
   * @returns {boolean} returns true if unindexed
   */
  async unindexObject({ object }: { object: IdObject }) {
    invariant(object, 'a node is required')
    invariant(object.id, 'a node must have an ID')

    const objectStoreKey = createObjectStoreKey(object)
    const subjectStoreKey = createSubjectStoreKey(object)
    const { redis } = this
    const id = String(object.id)
    // delete references where node is stored as a member in a labeled edge sorted set
    while (await redis.zcard(objectStoreKey)) {
      const members: string[] = await redis.zrangebyscore(objectStoreKey, -Infinity, Infinity, 'LIMIT', 0, 100)
      await map(members, labeledEdgeKey => {
        return Promise.all([redis.zremrangebyscore(labeledEdgeKey, id, id), redis.zrem(objectStoreKey, labeledEdgeKey)])
      })
    }
    // delete the labeled edge sorted sets of a node where it is the subject
    while (await redis.zcard(subjectStoreKey)) {
      const members: string[] = await redis.zrangebyscore(subjectStoreKey, -Infinity, Infinity, 'LIMIT', 0, 100)
      await map(members, labeledEdgeKey => {
        return Promise.all([redis.del(labeledEdgeKey), redis.zrem(subjectStoreKey, labeledEdgeKey)])
      })
    }

    return true
  }
}
