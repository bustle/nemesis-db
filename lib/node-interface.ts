import invariant from 'invariant'
import { map } from 'bluebird'
import { transform } from 'bluestream'
import MultiStream from 'multistream'
import {
  find,
  findIdByIndexedField,
  save,
  deleteNode,
  batchScan,
  paginate,
  GUID,
  TypedIdObject,
  decodeBase64,
  encodeBase64,
  UpdateNode,
  batchScanItr,
  scanItr,
  filterUndefinedFromUpdate,
  SavedFields,
} from './node-utils'
import { invariantNode } from '../invariant'
import { RedisLoader } from 'redis-loader'
import { GraphDBSchema } from './schema'
import { InterfaceDefinition, NodeDefinition } from './definitions'
import { Readable } from 'stream'
import { concat } from 'streaming-iterables'
import { GraphDB } from 'graphdb/types'

interface GraphNodeInterfaceLoaderOptions {
  readonly redis: RedisLoader
  readonly schema: GraphDBSchema
  readonly typeDef: InterfaceDefinition
}

interface PaginateOptions {
  readonly fields?: boolean | undefined
  readonly cursor?: string | undefined | null
  readonly count?: number | undefined
  readonly order?: 'ASC' | 'DESC' | undefined
}

function encodeInterfaceCursor(typeName: string, cursor: null | string) {
  return cursor ? encodeBase64(`${typeName}:${decodeBase64(cursor)}`) : null
}

export class GraphNodeInterfaceLoader<T extends GraphDB.Node> {
  readonly redis: RedisLoader
  readonly schema: GraphDBSchema
  readonly typeDef: InterfaceDefinition

  constructor({ redis, schema, typeDef }: GraphNodeInterfaceLoaderOptions) {
    invariant(redis, '"redis" is required')
    invariant(schema, '"schema" is required')
    invariant(typeDef, '"typeDef" is required')
    this.redis = redis
    this.schema = schema
    this.typeDef = typeDef
  }

  get implementerDefs() {
    return this.typeDef.implementers
  }
  get name() {
    return this.typeDef.name
  }
  get typeFields() {
    return this.typeDef.fields
  }
  get implementerNames() {
    return new Set(this.implementerDefs.map(({ name }) => name as string))
  }

  /**
   * Find a node
   */
  async find(id: GUID) {
    invariant(id, '"id" is required')
    const { schema, redis } = this
    const node = await find<T>(redis, schema, id)
    if (!node || !this.hasInstanceOf(node)) {
      return null
    }
    return node
  }

  /**
   * Find a node and throw if it doesn't exist
   */
  async FIND(id: GUID, CustomError: any = Error) {
    const node = await this.find(id)
    if (!node) {
      throw new CustomError(`${this.name}:${id} does not exist`)
    }
    return node
  }

  async findBy(field: string, value: any) {
    const { redis, name } = this
    invariant(field, '"key" is required')
    invariant(value !== undefined, `"value" is required for ${name}.findBy("${field}")`)
    invariant(!Array.isArray(value), `"value" cannot be an array for ${name}.findBy("${field}")`)

    for (const typeDef of this.implementerDefs) {
      const id = await findIdByIndexedField(redis, typeDef, field, value)
      if (!id) {
        continue
      }
      const node = await this.find(id)
      if (node) {
        return node
      }
    }
    return null
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
    return map(ids, id => this.FIND(id))
  }

  /**
   * paginate through all nodes of a type
   * @returns {Object} returns the `cursor` and `nodes` found
   */
  async paginate({ cursor, fields, count = 10, order }: PaginateOptions) {
    if (cursor === null) {
      return { cursor: null, nodes: [] }
    }
    const { redis, schema, implementerDefs, implementerNames } = this
    const paginationArgs = { schema, redis, cursor, fields, count, order }
    const sortedImpNames = [...implementerNames].sort()
    // if it's the beginning just do a normal paginate
    if (!cursor) {
      const firstImplementer = implementerDefs.find(imp => imp.name === sortedImpNames[0])
      const { cursor: nextCursor, nodes } = await paginate({ ...paginationArgs, typeDef: firstImplementer })
      return { cursor: encodeInterfaceCursor(firstImplementer.name, nextCursor), nodes }
    }
    // tslint:disable-next-line:prefer-const
    let [typeName, typeCursor] = decodeBase64(cursor).split(':')
    typeCursor = JSON.parse(typeCursor)
    const implementer = implementerDefs.find(imp => imp.name === typeName)
    // we're in the middle of a pagination
    const { cursor: nextCursor, nodes } = await paginate({
      ...paginationArgs,
      typeDef: implementer,
      cursor: typeCursor && encodeBase64(typeCursor),
    })
    // check to see if we're at the end of an implementer's pagination or at the end of the pagination
    // if nextCursor is null or there's not enough nodes and there are more types to read from
    if ((nextCursor === null || nodes.length < count) && sortedImpNames[sortedImpNames.length - 1] !== typeName) {
      const needsMoreNodes = nodes.length < count
      const nextCount = needsMoreNodes ? count - nodes.length : 1
      const nextImpName = sortedImpNames[sortedImpNames.indexOf(typeName) + 1]
      const nextImplementer = implementerDefs.find(imp => imp.name === nextImpName)
      // tslint:disable-next-line:prefer-const
      let { cursor: finalCursor, nodes: extraNodes } = await paginate({
        ...paginationArgs,
        typeDef: nextImplementer,
        cursor: undefined,
        count: nextCount,
      })
      finalCursor = needsMoreNodes ? finalCursor : encodeBase64((extraNodes[0] as any).id)
      const finalIntCursor = encodeInterfaceCursor(nextImpName, finalCursor)
      return { cursor: finalIntCursor, nodes: needsMoreNodes ? [...nodes, ...extraNodes] : nodes }
    }
    return { cursor: encodeInterfaceCursor(typeName, nextCursor), nodes }
  }

  async delete(id: GUID) {
    const { redis, schema } = this
    await this.FIND(id)
    return deleteNode({ redis, schema, id })
  }

  /**
   * Counts the number of typed nodes of a type
   */
  async count() {
    const { name, redis } = this
    return redis.zcard(`${name.toLowerCase()}:index:id`)
  }

  /**
   * Updates an existing node in the graph
   * @param {Object} update Requires an `id` to update, missing fields are not modified
   * @returns {Node}
   */
  async update(update: UpdateNode<T>) {
    invariant(update, '"update" must be an object')
    invariant(update.id, `"update.id" is required`)
    const { redis, schema, name } = this
    const existingNode = await this.find(update.id)
    if (!existingNode) {
      throw new Error(`${name}:${update.id} does not exist`)
    }

    invariant(
      this.hasInstanceOf(existingNode),
      `Wrong graphLoader for ${existingNode._nodeType}:${update.id} not an instance of ${name}`
    )

    if (update._nodeType) {
      invariant(
        existingNode._nodeType === update._nodeType,
        `Cannot change ${existingNode._nodeType}:${update.id} to a ${update._nodeType}`
      )
    }

    return save(redis, schema, {
      ...existingNode,
      updatedAt: Date.now(),
      ...filterUndefinedFromUpdate(update),
    })
  }

  /**
   * Does a redis scan and returns a stream of the nodes that implements the interface
   */
  scan({ fields = false, indexedField = 'id' } = {}): Readable {
    if (typeof arguments[0] === 'string') {
      throw new Error('scan() now takes an options object { fields?: boolean }')
    }
    const unBatch = transform(async function(objects: any[]) {
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
    const { redis, schema } = this
    return concat(
      ...this.implementerDefs.map(typeDef => scanItr({ redis, schema, typeDef, fields, indexedField, batchSize }))
    ) as any
  }

  /**
   * Does a redis scan and returns a batched stream of the nodes that implements the interface
   */
  batchScan({ batchSize = 500, fields = false, indexedField = 'id' } = {}): Readable {
    invariant(typeof batchSize === 'number', '"opts.batchSize" must be a number')
    invariant(typeof fields === 'boolean', '"opts.fields" must be a boolean')
    const { redis, schema } = this
    const streams = this.implementerDefs.map((typeDef: NodeDefinition) => () =>
      batchScan({ redis, schema, typeDef, batchSize, fields, indexedField })
    )
    return new (MultiStream as any)(streams, {
      highwaterMark: 1,
      objectMode: true,
    })
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
    const { redis, schema } = this
    return concat(
      ...this.implementerDefs.map(typeDef => batchScanItr({ redis, schema, typeDef, batchSize, fields, indexedField }))
    )
  }

  /**
   * Returns true if the node implements the interface
   * @param {Node} node to check against interface
   */
  hasInstanceOf(node: SavedFields): node is T
  hasInstanceOf(node: TypedIdObject): boolean
  hasInstanceOf(node: TypedIdObject) {
    invariantNode(node)
    invariant(typeof node._nodeType === 'string', '"node._nodeType" must be a string')
    const nodeType = node._nodeType
    return this.implementerNames.has(nodeType)
  }
}
