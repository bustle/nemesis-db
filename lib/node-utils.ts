import invariant from 'invariant'
import bluebird from 'bluebird'
import { pick } from 'lodash'
import { transform } from 'bluestream'
import { invariantNode } from '../invariant'
import { findDependentEdgeDefs } from './edge-utils'
import { GraphEdgeLoader, GraphDBEdgeFields } from './edge'
import { LabeledEdgeLoader } from './labeled-edge'
import { GraphDBSchema } from './schema'
import { NodeDefinition, InterfaceDefinition, FieldDefinition, EdgeDefinition, FieldDefMap } from './definitions'
import { RedisLoader } from 'redis-loader'
import { Readable } from 'stream'
import { getConfig } from 'lib/config'
import { batch, collect, parallelMap, pipeline } from 'streaming-iterables'
import { GraphDB } from 'graphdb/types'

const ASC = 'ASC'
const DESC = 'DESC'
const NODE_FIELDS_PREFIX = 'node:fields:'

export const STATES = {
  DELETING: 'deleting',
  DELETED: 'deleted',
}

export type GUID = number | string

export interface IdObject {
  readonly id: GUID
}

export interface TypedIdObject {
  readonly _nodeType: string
  readonly id: GUID
}

export interface SavedFields {
  readonly _nodeType: string
  readonly id: GUID
  readonly createdAt: number
  readonly updatedAt: number
}

export interface GraphDBNode {
  readonly _nodeType: string
  readonly id: GUID
  readonly createdAt: number
  readonly updatedAt: number
  [key: string]: any
}

type MutatablePartial<T> = { -readonly [P in keyof T]+?: T[P] }
export type UpdateNode<T extends IdObject> = IdObject & MutatablePartial<T>

export interface CreateGraphDBNodeBase {
  readonly id?: never
  readonly _nodeType?: string
}
interface BaseNodeUtilOptions {
  readonly redis: RedisLoader
  readonly schema: GraphDBSchema
}

interface SerializedFields {
  [key: string]: string | null
}

export function filterUndefinedFromUpdate<T>(data: T): T {
  const output: any = {}
  for (const [key, value] of Object.entries(data)) {
    if (value === undefined) {
      continue
    }
    output[key] = value
  }
  return output
}

export async function getNextId(redis: RedisLoader) {
  const { waitReplicaCount = 0, waitReplicaTimeout = 500 } = await getConfig()
  const id: GUID = String(await redis.incr(`graph:counter:id`))
  await redis.wait(waitReplicaCount, waitReplicaTimeout)
  return id
}

const isNumericField = (fieldDef: FieldDefinition) => fieldDef.type === 'number'
const isIndexedField = (fieldDef: FieldDefinition) => fieldDef.index

/**
 * Find a node of an unknown type
 */
export async function find<T extends GraphDB.Node>(redis: RedisLoader, schema: GraphDBSchema, id: GUID) {
  invariant(redis, '"redis" is required')
  invariant(schema, '"schema" is required')
  invariant(id, '"id" is required')

  const attributes = await redis.hgetall(`${NODE_FIELDS_PREFIX}${id}`)

  if (!attributes.id) {
    return null
  }

  const nodeType = schema.nodeDefs[attributes._nodeType]
  invariant(nodeType, `Cannot find type named "${attributes._nodeType}". id:${id}`)

  return deserializeFields<T>(nodeType, attributes)
}

export const encodeBase64 = (val: string | number) => Buffer.from(String(val)).toString('Base64')
export const decodeBase64 = (str: string) => Buffer.from(str, 'base64').toString('utf8')

interface PaginateOptions extends BaseNodeUtilOptions {
  readonly typeDef: NodeDefinition
  readonly fields?: boolean | undefined
  readonly cursor?: string | undefined | null
  readonly count?: number | undefined
  readonly order?: 'ASC' | 'DESC' | undefined
}

/**
 * paginate through all nodes of a type
 * @returns {Object} returns the `cursor` and `nodes` found
 */
export async function paginate<T extends GraphDB.Node>({
  typeDef,
  redis,
  schema,
  cursor,
  fields = false,
  count = 10,
  order = ASC,
}: PaginateOptions) {
  invariant(redis, '"redis" is required')
  invariant(schema, '"schema" is required')
  invariant(typeDef, '"typeDef" is required')
  invariant(typeof fields === 'boolean', '"opts.fields" must be boolean')
  invariant([ASC, DESC].includes(order), `"order" value must be either ${ASC} or ${DESC}`)
  if (cursor === null) {
    return { cursor: null, nodes: [] }
  }

  const decodedCursor = cursor ? decodeBase64(cursor) : undefined

  const { name } = typeDef

  let redisIds: string[]
  if (order === ASC) {
    const min = decodedCursor ? `[${decodedCursor}\xff` : `[0\xff`
    redisIds = await redis.zrangebylex(`${name.toLowerCase()}:index:id`, min, '[\xff', 'LIMIT', 0, count)
  } else {
    const max = decodedCursor ? `(${decodedCursor}` : `(\xff`
    redisIds = await redis.zrevrangebylex(`${name.toLowerCase()}:index:id`, max, '[', 'LIMIT', 0, count)
  }
  const lastId: void | string = redisIds[redisIds.length - 1]
  const nextCursor: null | string = lastId ? encodeBase64(lastId) : null

  const nodes = fields
    ? await bluebird.map(redisIds, id => find<T>(redis, schema, id))
    : redisIds.map(id => ({ id, _nodeType: name } as TypedIdObject))

  return { cursor: nextCursor, nodes }
}

interface BatchScanOptions extends BaseNodeUtilOptions {
  readonly batchSize: number
  readonly fields?: boolean
  readonly indexedField: string
  readonly typeDef: NodeDefinition
}

export function batchScan({
  redis,
  schema,
  typeDef,
  batchSize,
  fields = false,
  indexedField,
}: BatchScanOptions): Readable {
  invariant(redis, '"redis" is required')
  invariant(schema, '"schema" is required')
  invariant(typeDef, '"typeDef" is required')
  invariant(typeof batchSize === 'number', '"opts.batchSize" must be a number')
  invariant(typeof fields === 'boolean', '"opts.fields" must be a boolean')
  invariant(typeof indexedField === 'string', '"opts.indexedField" must be a string')
  const { name, fields: fieldDefs } = typeDef
  const fieldDef = fieldDefs[indexedField]
  if (indexedField !== 'id') {
    invariant(fieldDef, `"${indexedField}" is not a valid field for type: ${name}`)
    invariant(isNumericField(fieldDef), `"${indexedField}" is not a numeric field for type: ${name}`)
    invariant(isIndexedField(fieldDef), `"${indexedField}" is not indexed field for type: ${name}`)
  }

  const indexName = `${name.toLowerCase()}:index:${indexedField}`
  const loadObjects = transform(redisIds => {
    if (redisIds.length === 0) {
      return
    }
    const nodes: Array<TypedIdObject | Promise<TypedIdObject | null>> = []
    for (let i = 0; i < redisIds.length; i += 2) {
      const id = redisIds[i]
      if (fields) {
        nodes.push(find(redis, schema, id))
      } else {
        nodes.push({ id, _nodeType: name })
      }
    }
    if (fields) {
      return Promise.all(nodes)
    }
    return nodes
  })
  const stream = redis.zscanStream(indexName, { count: batchSize }).pipe(loadObjects)
  return stream
}

interface ScanOptions extends BaseNodeUtilOptions {
  readonly batchSize: number
  readonly typeDef: NodeDefinition
  readonly indexedField: string
  readonly fields?: boolean
}

/**
 * Yield nodes one by one, look up the index by batch size and find the nodes one by one if fields:true
 */
export async function* scanItr<T extends GraphDB.Node>({
  redis,
  schema,
  typeDef,
  indexedField,
  batchSize,
  fields = false,
}: ScanOptions) {
  for await (const batch of batchScanItr<T>({ redis, schema, typeDef, batchSize, indexedField, fields })) {
    yield* batch
  }
}

/**
 * Yield nodes in batches, look up the index by batch size and find all the nodes in a batch when index:true
 */

export async function* batchScanItr<T extends GraphDB.Node>({
  redis,
  schema,
  typeDef,
  batchSize,
  indexedField,
  fields = false,
}: BatchScanOptions) {
  const { name, fields: fieldDefs } = typeDef
  const fieldDef = fieldDefs[indexedField]
  if (indexedField !== 'id') {
    invariant(fieldDef, `"${indexedField}" is not a valid field for type: ${name}`)
    invariant(isNumericField(fieldDef), `"${indexedField}" is not a numeric field for type: ${name}`)
    invariant(isIndexedField(fieldDef), `"${indexedField}" is not indexed field for type: ${name}`)
  }
  const indexName = `${name.toLowerCase()}:index:${indexedField}`

  const finder = fields
    ? ([id]: GUID[]) => find<T>(redis, schema, id) as Promise<T>
    : ([id]: GUID[]) => ({ id, _nodeType: name } as Pick<T, 'id' | '_nodeType'>)
  for await (const page of redis.zscanIterable(indexName, { count: batchSize })) {
    const nodes = await pipeline(() => page, batch(2), parallelMap<any, any>(Infinity, finder), collect)
    if (nodes.length === 0) {
      return
    }
    yield nodes as T[] | Array<Pick<T, 'id' | '_nodeType'>>
  }
}

async function removeFieldFromNumericIndexes(redis: RedisLoader, nodeDef: NodeDefinition, id: GUID, field: string) {
  const typeInterfaces = nodeDef.interfaces as InterfaceDefinition[]
  const asyncWork: Array<Promise<any>> = []
  const lowerName = nodeDef.name.toLowerCase()
  asyncWork.push(redis.zrem(`${lowerName}:index:${field}`, String(id)))
  typeInterfaces.forEach(({ fields, name }) => {
    if (fields[field] && fields[field].index) {
      asyncWork.push(redis.zrem(`${name.toLowerCase()}:index:${field}`, String(id)))
    }
  })
  await Promise.all(asyncWork)
}

async function upsertFieldToNumericIndexes(
  redis: RedisLoader,
  nodeDef: NodeDefinition,
  id: GUID,
  field: string,
  newValue: string
) {
  const typeInterfaces = nodeDef.interfaces as InterfaceDefinition[]
  const asyncWork: Array<Promise<any>> = []
  const lowerName = nodeDef.name.toLowerCase()
  asyncWork.push(redis.zadd(`${lowerName}:index:${field}`, String(newValue), String(id)))
  typeInterfaces.forEach(({ fields, name }) => {
    if (fields[field] && fields[field].index) {
      asyncWork.push(redis.zadd(`${name.toLowerCase()}:index:${field}`, String(newValue), String(id)))
    }
  })
  await Promise.all(asyncWork)
}

async function removeFieldFromStringIndex(redis: RedisLoader, nodeDef: NodeDefinition, field: string, value: string) {
  const lowerName = nodeDef.name.toLowerCase()
  await redis.hdel(`${lowerName}:index:${field}`, value)
}

async function addFieldToStringIndex(
  redis: RedisLoader,
  nodeDef: NodeDefinition,
  id: GUID,
  field: string,
  value: string
) {
  const lowerName = nodeDef.name.toLowerCase()
  await redis.hset(`${lowerName}:index:${field}`, value, id)
}

async function addToIdIndexes(redis: RedisLoader, nodeDef: NodeDefinition, id: GUID) {
  const asyncWork: Array<Promise<any>> = []
  const lowerName = nodeDef.name.toLowerCase()
  const typeInterfaces = nodeDef.interfaces as InterfaceDefinition[]
  typeInterfaces.forEach(({ name }) => {
    asyncWork.push(redis.zadd(`${name.toLowerCase()}:index:id`, '0', String(id)))
  })
  asyncWork.push(redis.zadd(`${lowerName}:index:id`, '0', String(id)))
  await Promise.all(asyncWork)
}

async function removeIdFromIndex(redis: RedisLoader, nodeDef: NodeDefinition, id: GUID) {
  const asyncWork: Array<Promise<any>> = []
  const lowerName = nodeDef.name.toLowerCase()
  asyncWork.push(redis.zrem(`${lowerName}:index:id`, id))
  const typeInterfaces = nodeDef.interfaces as InterfaceDefinition[]
  typeInterfaces.forEach(({ name }) => {
    asyncWork.push(redis.zrem(`${name.toLowerCase()}:index:id`, id))
  })
  await Promise.all(asyncWork)
}

async function persistSerilizedNode(redis: RedisLoader, id: GUID, serializedAttributes: any) {
  const fieldsToSet = Object.entries(serializedAttributes).reduce(
    (memo, entry) => {
      const [fieldName, value] = entry
      if (value !== null) {
        memo[fieldName] = value
      }
      return memo
    },
    {} as any
  )
  const fieldsToDelete = Object.keys(serializedAttributes).filter(fieldName => serializedAttributes[fieldName] === null)

  const asyncWork: Array<Promise<any>> = []

  // Set new fields and delete null fields
  asyncWork.push(
    redis.hmset(`${NODE_FIELDS_PREFIX}${id}`, fieldsToSet),
    ...fieldsToDelete.map(fieldName => redis.hdel(`${NODE_FIELDS_PREFIX}${id}`, fieldName))
  )
  await Promise.all(asyncWork)
}

/**
 * Save a node. Will overwrite any existing nodes with the same id. Does not handle changing the _nodeType of a node.
 */
export async function save<T extends GraphDBNode>(
  redis: RedisLoader,
  schema: GraphDBSchema,
  node: GraphDBNode
): Promise<T> {
  invariant(redis, '"redis" is required')
  invariant(schema, '"schema" is required')
  invariant(node, '"node" must be an object')
  invariant(node.id, '"node.id" must exist')
  invariant(node._nodeType, '"node._nodeType" must exist')

  // Ensure we're not changing the nodeType or a node that's being deleted
  const [existingNodeType, state] = await redis.hmget(`${NODE_FIELDS_PREFIX}${node.id}`, '_nodeType', '_state')
  if (existingNodeType) {
    invariant(
      existingNodeType === node._nodeType,
      `Cannot change ${existingNodeType}:${node.id} to a ${node._nodeType}`
    )
  }
  invariant(state !== STATES.DELETING, 'Cannot update a node being deleted')

  const typeDef = schema.nodeDefs[node._nodeType]
  const typeFields = typeDef.fields
  const attributes = { ...node }

  const indexedFieldNames = Object.keys(typeFields).filter(field => typeFields[field].index)
  const { id } = attributes

  // read the values for any field that has an index:true on it
  let oldIndexedFieldValues = {} as { [key: string]: string | null }
  if (indexedFieldNames.length > 0) {
    const values: Array<string | null> = await redis.hmget(`${NODE_FIELDS_PREFIX}${id}`, ...indexedFieldNames)
    oldIndexedFieldValues = values.reduce(
      (memo, fieldValue, index) => {
        const fieldName = indexedFieldNames[index]
        memo[fieldName] = fieldValue
        return memo
      },
      {} as { [key: string]: string | null }
    )
  }

  const serializedAttributes = serializeFields(typeDef, attributes)

  const changedIndexedFields: string[] = []
  for (const name of indexedFieldNames) {
    if (oldIndexedFieldValues[name] !== serializedAttributes[name]) {
      changedIndexedFields.push(name)
    }
  }

  const asyncWork: Array<Promise<any>> = []

  // Set new fields and delete null fields
  asyncWork.push(persistSerilizedNode(redis, id, serializedAttributes))

  // Set ID indexes
  asyncWork.push(addToIdIndexes(redis, typeDef, id))

  // update the indexes of fields that have changed
  for (const field of changedIndexedFields) {
    const oldValue = oldIndexedFieldValues[field] as string | null
    const newValue = serializedAttributes[field] as string | null

    if (isNumericField(typeFields[field])) {
      if (oldValue !== null && newValue === null) {
        asyncWork.push(removeFieldFromNumericIndexes(redis, typeDef, id, field))
      } else if (newValue !== null) {
        asyncWork.push(upsertFieldToNumericIndexes(redis, typeDef, id, field, newValue))
      }
    } else {
      if (oldValue !== null) {
        asyncWork.push(removeFieldFromStringIndex(redis, typeDef, field, oldValue))
      }
      if (newValue !== null) {
        asyncWork.push(addFieldToStringIndex(redis, typeDef, id, field, newValue))
      }
    }
  }

  await Promise.all(asyncWork)

  return deserializeFields<T>(typeDef, serializedAttributes)
}

export async function deleteNode<T extends GraphDB.Node>({
  id,
  redis,
  schema,
}: {
  readonly id: GUID
  readonly redis: RedisLoader
  readonly schema: GraphDBSchema
}) {
  invariant(redis, '"redis" is required')
  invariant(schema, '"schema" is required')

  invariant(typeof id === 'number' || typeof id === 'string', '"id" must be a number or string id')
  const node = await find<T>(redis, schema, id)
  if (!node) {
    throw new Error(`Node with ${id} does not exist`)
  }

  const edgeLoader = new GraphEdgeLoader({ schema, redis })
  const labeledEdgeLoader = new LabeledEdgeLoader({ schema, redis })

  // add flag that node is to be deleted
  await redis.hmset(`${NODE_FIELDS_PREFIX}${id}`, { _state: STATES.DELETING })

  // delete dependent nodes
  const dependentEdges = findDependentEdgeDefs(schema, schema.registry[node._nodeType] as NodeDefinition)
  await bluebird.each(dependentEdges, async ({ name: predicate }) => {
    while (await edgeLoader.count({ subject: node, predicate })) {
      const edges = await edgeLoader.find({ subject: node, predicate })
      await bluebird.each(edges, async ({ object }) => {
        await deleteNode({ redis, schema, id: object.id })
      })
    }
  })

  // delete the edges and indexes
  await edgeLoader.deleteAllForNode({ id })
  await labeledEdgeLoader.unindexObject({ object: { id } })
  await removeFromNodeIndexedFields(redis, schema, node)

  // delete the node
  await redis.del(`${NODE_FIELDS_PREFIX}${id}`)
  ;(node as any)._state = STATES.DELETED
  return node
}

export async function removeFromNodeIndexedFields(redis: RedisLoader, schema: GraphDBSchema, node: GraphDBNode) {
  invariant(redis, `"redis" is required`)
  invariant(schema, `"schema" is required`)
  invariant(node, `"node" is required`)
  const { id } = node
  const typeDef = schema.nodeDefs[node._nodeType]
  invariant(typeDef, `unable to determine type ${node._nodeType}`)
  const typeFields = typeDef.fields
  const indexedFieldNames = Object.keys(typeFields).filter(field => typeFields[field].index)
  const asyncWork: Array<Promise<any>> = []

  // remove from field indexes
  indexedFieldNames.forEach(field => {
    const value = node[field]
    if (value === null) {
      return
    }
    if (isNumericField(typeFields[field])) {
      asyncWork.push(removeFieldFromNumericIndexes(redis, typeDef, id, field))
    } else {
      asyncWork.push(removeFieldFromStringIndex(redis, typeDef, field, value))
    }
  })

  // remove from id indexes
  asyncWork.push(removeIdFromIndex(redis, typeDef, id))
  await Promise.all(asyncWork)
}

export async function findIdByIndexedField(
  redis: RedisLoader,
  typeDef: InterfaceDefinition | NodeDefinition,
  field: string,
  value: any
) {
  invariant(redis, '"redis" is required')
  invariant(typeDef, '"typeDef" is required')
  invariant(field, '"field" is required')
  const { name, fields } = typeDef
  invariant(value !== undefined, `"value" is required for ${name}.findBy("${field}")`)
  invariant(!Array.isArray(value), `"value" cannot be an array for ${name}.findBy("${field}")`)

  const fieldDef = fields[field]
  invariant(fieldDef, `Type "${name}" does not have a field named "${field}"`)
  invariant(fieldDef.index === true, `Field "${field}" is not indexed on type "${name}"`)
  invariant(
    fieldDef.type !== 'number',
    `Field "${field}" on type "${name}" is stored in a numerical index. Use findByRange instead.`
  )
  return redis.hget(`${name.toLowerCase()}:index:${field}`, value)
}

export function serializeField(fieldDef: FieldDefinition, value: any): string | null {
  let dbValue
  if (value === undefined && fieldDef.defaultValue !== undefined) {
    return fieldDef.defaultValue
  } else {
    dbValue = value
  }

  if (dbValue === undefined || dbValue === null) {
    if (!fieldDef.nullable) {
      throw new TypeError(`value is null or undefined and not nullable`)
    }
    return null
  }

  switch (fieldDef.type) {
    case 'boolean':
      if (typeof dbValue !== 'boolean') {
        throw new TypeError(`invalid boolean`)
      }
      return dbValue ? '1' : '0'
    case 'JSON':
      // whatever we'll take it
      return JSON.stringify(dbValue)
    case 'number':
      if (typeof dbValue !== 'number' || isNaN(dbValue)) {
        throw new TypeError(`invalid number`)
      }
      return String(dbValue)
    case 'string':
      if (typeof dbValue !== 'string') {
        throw new TypeError(`invalid string`)
      }
      return String(dbValue)
    case 'ID':
      if (typeof dbValue !== 'string' && typeof dbValue !== 'number') {
        throw new TypeError(`invalid ID must be string or number`)
      }
      return String(dbValue)
    default:
      throw new Error(`type "${fieldDef.type}" is unknown`)
  }
}

export function serializeFields(typeDef: NodeDefinition, fields: { [key: string]: any }): SerializedFields
export function serializeFields(typeDef: EdgeDefinition, fields: { [key: string]: any }): SerializedFields | null
export function serializeFields(
  typeDef: NodeDefinition | EdgeDefinition,
  fields: { [key: string]: any }
): SerializedFields | null {
  const typeFields = typeDef.fields
  if (!typeFields) {
    return null
  }
  const serialized: SerializedFields = {}

  // if any keys exist in the fields but not the typeDef, throw
  for (const fieldName of Object.keys(fields)) {
    if (fieldName === '_nodeType') {
      serialized._nodeType = fields[fieldName]
      continue
    }
    const fieldDef = typeFields[fieldName]
    if (!fieldDef) {
      throw new TypeError(`Unable to save field ${typeDef.name}.${fieldName} as it's not defined in graphdb`)
    }
  }

  for (const [fieldName, fieldDef] of Object.entries(typeFields)) {
    const value = fields[fieldName]

    try {
      serialized[fieldName] = serializeField(fieldDef, value)
    } catch (error) {
      throw new TypeError(`Unable to serialize ${typeDef.name}.${fieldName}: ${error.message}`)
    }
  }
  return serialized
}
export function deserializeFields<T extends GraphDBNode>(typeDef: NodeDefinition, fields: any): T
export function deserializeFields(typeDef: EdgeDefinition, fields: any): GraphDBEdgeFields
export function deserializeFields(typeDef: NodeDefinition | EdgeDefinition, fields: any) {
  const deserialized: { [key: string]: any } = {}
  if (typeDef.type === 'node') {
    deserialized.id = fields.id
    deserialized._nodeType = typeDef.name
  }
  for (const [fieldName, fieldDef] of Object.entries(typeDef.fields as FieldDefMap)) {
    const dbVal = fields[fieldName]
    if ((dbVal === undefined || dbVal === null) && fieldDef.nullable) {
      deserialized[fieldName] = null
      continue
    }
    const value = dbVal === undefined || dbVal === null ? fieldDef.missingFieldValue : dbVal
    switch (fieldDef.type) {
      case 'boolean':
        deserialized[fieldName] = value === '1'
        break
      case 'JSON':
        if (value === '') {
          deserialized[fieldName] = null
          break
        }
        try {
          deserialized[fieldName] = JSON.parse(value)
        } catch (e) {
          throw new Error(`deserializeAttributes: Invalid JSON for ${typeDef.name}(${fields.id}).${fieldName}`)
        }
        break
      case 'number':
        deserialized[fieldName] = value ? Number(value) : null
        break
      default:
        deserialized[fieldName] = value
    }
  }
  return deserialized
}

export function hasInstanceOf(type: InterfaceDefinition | NodeDefinition, node: TypedIdObject) {
  invariantNode(node)
  invariant(type, '"type" is required')
  if (type.type === 'node') {
    return node._nodeType === type.name
  }
  return type.implementerNames.includes(node._nodeType)
}

export async function findNodeType(redis: RedisLoader, { id }: IdObject) {
  invariant(redis, '"redis" is required')
  invariant(id, 'a partial node with an id is required')
  const nodeType = await redis.hget(`${NODE_FIELDS_PREFIX}${id}`, '_nodeType')
  if (!nodeType) {
    throw new Error(`Node:${id} does not exist`)
  }
  return nodeType
}

export async function changeNodeType(
  redis: RedisLoader,
  schema: GraphDBSchema,
  id: GUID,
  target: string,
  data: any = {}
) {
  const targetDef = schema.registry[target]
  if (!(targetDef instanceof NodeDefinition)) {
    throw new Error(`target type "${target}" must be a NodeDefinition`)
  }
  const node = await find(redis, schema, id)
  if (!node) {
    throw new Error(`No such Node:${id}`)
  }
  const newNodeFields = Object.keys(targetDef.fields)
  const newNode = {
    ...pick(node, ...newNodeFields),
    ...data,
    _nodeType: targetDef.name,
  }
  const serializedNode = serializeFields(targetDef, newNode)
  await removeFromNodeIndexedFields(redis, schema, node)
  await persistSerilizedNode(redis, id, serializedNode)
  const asyncWork = [addToIdIndexes(redis, targetDef, id)]
  const indexedFieldEntries = Object.entries(targetDef.fields).filter(([_field, fieldDef]) => fieldDef.index)
  for (const [field, fieldDef] of indexedFieldEntries) {
    const value = serializedNode[field] as string | null
    if (value === null) {
      continue
    }
    if (fieldDef.type === 'number') {
      asyncWork.push(upsertFieldToNumericIndexes(redis, targetDef, id, field, value))
    } else {
      asyncWork.push(addFieldToStringIndex(redis, targetDef, id, field, value))
    }
  }

  await Promise.all(asyncWork)
}
