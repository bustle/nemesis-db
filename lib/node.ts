import invariant from 'invariant'
import { transform } from 'bluestream'
import {
  find,
  findIdByIndexedField,
  save,
  deleteNode,
  removeFromNodeIndexedFields,
  batchScan,
  GUID,
  paginate,
  UpdateNode,
  getNextId,
  batchScanItr,
  scanItr,
  CreateGraphDBNodeBase,
  filterUndefinedFromUpdate,
  TypedIdObject,
  SavedFields,
} from './node-utils'
import { invariantNode } from '../invariant'
import { RedisLoader } from 'redis-loader'
import { GraphDBSchema, EdgeDefs } from './schema'
import { NodeDefinition } from './definitions'
import { Readable } from 'stream'
import { GraphDB } from 'graphdb/types'

interface GraphNodeLoaderOptions {
  readonly redis: RedisLoader
  readonly schema: GraphDBSchema
  readonly typeDef: NodeDefinition
}

interface PaginateOptions {
  readonly fields?: boolean | undefined
  readonly cursor?: string | undefined | null
  readonly count?: number | undefined
  readonly order?: 'ASC' | 'DESC' | undefined
}

/**
 * Base Class for node based graph objects
 *
 * This class handles the communication with redis regarding loading, creating and updating nodes.
 */
export class GraphNodeLoader<T extends GraphDB.Node, R extends CreateGraphDBNodeBase> {
  readonly redis: RedisLoader
  readonly schema: GraphDBSchema
  readonly typeDef: NodeDefinition
  constructor({ redis, schema, typeDef }: GraphNodeLoaderOptions) {
    invariant(redis, '"redis" is required')
    invariant(schema, '"schema" is required')
    invariant(typeDef, '"typeDef" is required')
    this.redis = redis
    this.schema = schema
    this.typeDef = typeDef
  }

  get edgeDefs(): EdgeDefs {
    return this.schema.edgeDefs
  }
  get name(): string {
    return this.typeDef.name
  }
  get typeFields() {
    return this.typeDef.fields
  }
  get indexedFieldNames() {
    return Object.keys(this.typeFields).filter(field => this.typeFields[field].index)
  }

  async _save(node: any) {
    return save<T>(this.redis, this.schema, node)
  }

  /**
   * Creates a node in the graph with properties passed in. Will generate an `id`, and add `updatedAt` and `createdAt` if missing.
   */
  async create(properties: R) {
    invariant(properties, '"properties" must be an object')
    invariant(
      !(properties as any).id,
      `"properties.id" should be undefined for new nodes. Use "_save" if you need to manually set the id.`
    )
    const { redis, schema, name } = this
    if ((properties as any)._nodeType) {
      invariant(
        name === (properties as any)._nodeType,
        `Wrong loader to save node of type "${(properties as any)._nodeType}" this is "${name}"`
      )
    }
    const id = await getNextId(redis)

    const exists = await redis.exists(`node:fields:${id}`)
    invariant(!exists, `Node:${id} already exists unable to create node. Something very bad has just happened`)

    const now = Date.now()
    return save<T>(redis, schema, {
      updatedAt: now,
      createdAt: now,
      ...properties,
      id,
      _nodeType: name,
    })
  }

  /**
   * Updates an existing node in the graph
   */
  async update(update: UpdateNode<T>) {
    invariant(update, '"update" must be an object')
    invariant(update.id, `"update.id" is required`)
    const { redis, schema, name } = this
    const existingNode = await this.find(update.id)
    if (!existingNode) {
      throw new Error(`${name}:${update.id} does not exist`)
    }
    if (update._nodeType) {
      invariant(
        existingNode._nodeType === update._nodeType,
        `Cannot change ${existingNode._nodeType}:${update.id} to a ${update._nodeType}`
      )
    }
    // ignore undefined values in updates

    return save<T>(redis, schema, {
      ...existingNode,
      updatedAt: Date.now(),
      ...filterUndefinedFromUpdate(update),
    })
  }

  /**
   * find a node
   */
  async find(id: GUID) {
    invariant(id, '"id" is required')
    const { redis, schema } = this
    const node = await find<T>(redis, schema, id)
    if (!node || !this.hasInstanceOf(node)) {
      return null
    }
    return node
  }

  /**
   * Find and throw an erorr if it's missing
   */
  async FIND(id: GUID, CustomError: any = Error) {
    const node = await this.find(id)
    if (!node) {
      throw new CustomError(`${this.name}:${id} does not exist`)
    }
    return node
  }

  /**
   * Load an object by an indexed value
   * @param {string} field indexed field to lookup the value on
   * @param {string} value the value to lookup
   * @returns {Node|null}
   */
  async findBy(field: string, value: string | number) {
    const { redis, typeDef } = this
    const id = await findIdByIndexedField(redis, typeDef, field, value)
    if (!id) {
      return null
    }
    return this.find(id)
  }

  async FINDBY(field: string, value: string, CustomError: any = Error) {
    const node = await this.findBy(field, value)
    if (!node) {
      throw new CustomError(`${this.name}.${field}:"${value}" does not exist`)
    }
    return node
  }

  async findByRange(key: string, min: number, max: number) {
    invariant(key, '"key" is required')
    invariant(min !== undefined, '"min" is required')
    invariant(min !== undefined, '"max" is required')
    const { name, redis, typeFields } = this
    const field = typeFields[key]

    invariant(field, `Type "${name}" does not have a field named "${key}"`)
    invariant(field.index === true, `Field "${key}" is not indexed on type "${name}"`)
    if (key !== 'id') {
      invariant(
        field.type === 'number',
        `Field "${key}" on type "${name}" is not stored in a numerical index. Use findBy instead.`
      )
    }
    const ids: GUID[] = await redis.zrangebyscore(`${name.toLowerCase()}:index:${key}`, min, max)
    return Promise.all(ids.map(id => this.FIND(id)))
  }

  /**
   * Counts the number of typed nodes of a type
   */
  async count(): Promise<number> {
    const { name, redis } = this
    return redis.zcard(`${name.toLowerCase()}:index:id`)
  }

  /**
   * Removes a node from its type's indexed fields
   */
  async _removeFromIndexedFields(id: GUID) {
    const node = await this.FIND(id)
    await removeFromNodeIndexedFields(this.redis, this.schema, node)
  }

  /**
   * paginate through all nodes of a type
   * @param {any} typeDef node or interface type
   * @param {string|null} cursor for the next offset
   * @param {boolean} fields boolean to look up node fields
   * @param {int} count how many nodes to return
   * @param {string} order ASC or DESC
   * @returns {Object} returns the `cursor` and `nodes` found
   */
  async paginate({ cursor, fields, count, order }: PaginateOptions) {
    const { redis, schema, typeDef } = this
    return paginate<T>({ typeDef, schema, redis, cursor, fields, count, order })
  }

  /**
   * Deletes a node and its associated edges
   */
  async delete(id: GUID) {
    const { redis, schema } = this
    await this.FIND(id)
    return deleteNode<T>({ redis, schema, id })
  }

  /**
   * Does a redis scan and returns a stream of nodes
   */
  scan({ fields = false, indexedField = 'id' } = {}): Readable {
    if (typeof arguments[0] === 'string') {
      throw new Error('scan() now takes an options object { fields?: boolean }')
    }
    const unBatch = transform(function(objects: any[]): void {
      objects.forEach(obj => this.push(obj))
    })
    return this.batchScan({ fields, indexedField }).pipe(unBatch)
  }

  scanItr({
    batchSize,
    fields,
    indexedField,
  }: {
    fields: true
    batchSize?: number
    indexedField?: string
  }): AsyncIterable<T>
  scanItr({
    batchSize,
    fields,
    indexedField,
  }: {
    fields?: false
    batchSize?: number
    indexedField?: string
  }): AsyncIterable<Pick<T, 'id' | '_nodeType'>>
  scanItr({ batchSize = 500, fields = false, indexedField = 'id' } = {}) {
    const { redis, schema, typeDef } = this
    return scanItr<T>({ redis, schema, typeDef, fields, indexedField, batchSize }) as any
  }

  /**
   * Does a redis scan and returns a batched stream of the indexed nodes
   */
  batchScan({ batchSize = 500, fields = false, indexedField = 'id' } = {}) {
    invariant(typeof batchSize === 'number', '"opts.batchSize" must be a number')
    invariant(typeof fields === 'boolean', '"opts.fields" must be a boolean')
    const { redis, schema, typeDef } = this
    return batchScan({ redis, schema, typeDef, batchSize, fields, indexedField })
  }

  batchScanItr({
    batchSize,
    fields,
    indexedField,
  }: {
    fields: true
    batchSize?: number
    indexedField?: string
  }): AsyncIterable<T[]>
  batchScanItr({
    batchSize,
    fields,
    indexedField,
  }: {
    fields?: false
    batchSize?: number
    indexedField?: string
  }): AsyncIterable<Array<Pick<T, 'id' | '_nodeType'>>>
  batchScanItr({ batchSize = 500, fields = false, indexedField = 'id' } = {}) {
    const { redis, schema, typeDef } = this
    return batchScanItr<T>({ redis, schema, typeDef, batchSize, fields, indexedField }) as any
  }
  hasInstanceOf(node: SavedFields): node is T
  hasInstanceOf(node: TypedIdObject): boolean
  hasInstanceOf(node: TypedIdObject) {
    invariantNode(node)
    invariant(typeof node._nodeType === 'string', '"node._nodeType" must be a string')
    return node._nodeType === this.name
  }
}
