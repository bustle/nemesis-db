import * as Redis from 'ioredis'
import * as messagePack from 'msgpack5'
import { compressData, decompressData } from './compression'
import { Compressor } from './compression/types'

export interface Node {
  readonly id: number
  readonly [key: string]: any
}

export interface GraphConfigInput {
  readonly compressors?: ReadonlyArray<Compressor>
  readonly edgePrefix?: string
  readonly guidKey?: string
  readonly labeledEdgePrefix?: string
  readonly nodeIndexKey?: string
  readonly nodeKeyPrefix?: string
}

export interface GraphConfig {
  readonly compressors: Map<string, Compressor>
  readonly edgePrefix: string
  readonly guidKey: string
  readonly labeledEdgePrefix: string
  readonly nodeIndexKey: string
  readonly nodeKeyPrefix: string
}

export interface EdgeInput {
  readonly object: number
  readonly predicate: string
  readonly subject: number
  readonly weight?: number
}

export interface Edge {
  readonly object: number
  readonly predicate: string
  readonly subject: number
  readonly weight: number
}

export interface LabeledEdge {
  readonly label: string
  readonly object: number
  readonly predicate: string
  readonly subject: number
}

export interface SubjectEdgeSearch {
  readonly limit?: number
  readonly offset?: number
  readonly predicate: string
  readonly subject: number
}

export interface ObjectEdgeSearch {
  readonly limit?: number
  readonly object: number
  readonly offset?: number
  readonly predicate: string
}

export interface LabeledEdgeSearch {
  readonly label: string
  readonly predicate: string
  readonly subject: number
}

export interface NodeScanOptions {
  readonly batchSize?: number
}

declare module 'ioredis' {
  interface Redis {
    readonly createEdge: (
      objectEdgeKey: string,
      subjectEdgeKey: string,
      subjectKey: string,
      objectKey: string,
      subject: number,
      object: number,
      weight: number
    ) => Promise<void>

    readonly createNode: (
      nodeKey: string,
      nodeIndexKey: string,
      id: number,
      compressor: string,
      data: Buffer
    ) => Promise<void>

    // tslint:disable-next-line:readonly-array
    readonly hmgetBuffer: (key: string, ...keys: string[]) => Promise<ReadonlyArray<Buffer>>
    readonly putNode: (nodeKey: string, id: number, compressor: string, data: Buffer) => Promise<void>
  }
}

// polyfill for missing async iterator symbol
/* istanbul ignore if */
if ((Symbol as any).asyncIterator === undefined) {
  ((Symbol as any).asyncIterator) = Symbol.for('asyncIterator')
}

function decodeError<T> (promise: Promise<T>): Promise<T> {
  return promise.catch(error => {
    if (error.name === 'ReplyError') {
      const firstLambdaPosition = error.message.indexOf('λ')
      const lastLambdaPosition = error.message.lastIndexOf('λ')
      const message = error.message.substring(firstLambdaPosition + 1, lastLambdaPosition)
      throw new Error(message)
    }
    throw error
  })
}

export class Graph {
  readonly config: GraphConfig
  readonly messagePack: messagePack.MessagePack
  readonly redis: Redis.Redis

  constructor (redis: string | Redis.Redis, config?: GraphConfigInput) {
    const {
      guidKey = 'counter:guid',
      nodeKeyPrefix = 'node:',
      edgePrefix = 'edge:',
      labeledEdgePrefix = 'label:',
      nodeIndexKey = 'index:node:id',
      compressors = []
    } = config || {}

    const compressorsMap: Map<string, Compressor> = new Map()
    for (const compressor of compressors) {
      compressorsMap.set(compressor.name, compressor)
    }

    this.config = {
      guidKey,
      nodeKeyPrefix,
      edgePrefix,
      nodeIndexKey,
      compressors: compressorsMap,
      labeledEdgePrefix
    }
    this.redis = typeof redis === 'string' ? new Redis(redis) : redis
    this.evalCommands()
    this.messagePack = messagePack()
  }

  async *allNodes ({ batchSize = 200 }: NodeScanOptions = {}): AsyncIterableIterator<Node> {
    let cursor = 0
    while (true) {
      const [nextCursor, redisIds] = await this.redis.zscan(this.config.nodeIndexKey, cursor, 'COUNT', batchSize)
      for (let i = 0; i < redisIds.length; i += 2) {
        const id = redisIds[i]
        const node = await this.findNode(id)
        /* istanbul ignore else */
        if (node) {
          yield node
        }
      }
      if (nextCursor === '0') {
        return
      }
      cursor = Number(nextCursor)
    }
  }

  async createEdge ({ subject, predicate, object, weight = 0 }: EdgeInput): Promise<Edge> {
    const objectEdgeKey = `${this.config.edgePrefix}:o:${object}:${predicate}`
    const subjectEdgeKey = `${this.config.edgePrefix}:s:${subject}:${predicate}`
    const subjectKey = this.nodeKey(subject)
    const objectKey = this.nodeKey(object)

    await decodeError(this.redis.createEdge(
      objectEdgeKey,
      subjectEdgeKey,
      subjectKey,
      objectKey,
      subject,
      object,
      weight
    ))
    return { subject, predicate, object, weight }
  }

  async createLabeledEdge (edge: LabeledEdge): Promise<LabeledEdge> {
    const { subject, predicate, label, object } = edge
    const OBJECT_EDGE_LABEL = 'o'
    const SUBJECT_EDGE_LABEL = 's'

    const namedEdgeKey = `${this.config.labeledEdgePrefix}:${subject}:${predicate}`
    const objectStoreKey = `${this.config.labeledEdgePrefix}:${OBJECT_EDGE_LABEL}:${object}`
    const subjectStoreKey = `${this.config.labeledEdgePrefix}:${SUBJECT_EDGE_LABEL}:${subject}`
    const score = await this.redis.zscore(namedEdgeKey, label)

    if (score) {
      if (Number(score) !== object) {
        // tslint:disable-next-line:max-line-length
        throw new Error(`Labeled Edge for subject:${subject} predicate:${predicate} label:"${label}" is already in use by node id:${score}`)
      }
      return edge
    }
    await Promise.all([
      this.redis.zadd(namedEdgeKey, `${object}`, label),
      // this.redis.zadd(objectStoreKey, `${subject}`, namedEdgeKey),
      // this.redis.zadd(subjectStoreKey, '0', namedEdgeKey)
    ])
    return edge
  }

  async findLabeledEdge (inputEdge: LabeledEdgeSearch): Promise<LabeledEdge|null> {
    const { label, subject, predicate } = inputEdge
    const namedEdgeKey = `${this.config.labeledEdgePrefix}:${subject}:${predicate}`
    const score = await this.redis.zscore(namedEdgeKey, label)
    if (!score) {
      return null
    }
    return {
      label,
      subject,
      predicate,
      object: Number(score)
    }
  }

  async createNode (attributes): Promise<Node> {
    if (attributes.id) {
      throw new Error('attributes already has an "id" property do you want putNode() or updateNode()?')
    }
    const id = await this.getNextId()
    const node = {
      ...attributes,
      id
    }

    const data = this.messagePack.encode(node).slice()

    const { name: compressorName, data: compressedData } = await compressData(data, this.config.compressors)

    await decodeError(this.redis.createNode(
      this.nodeKey(id),
      this.config.nodeIndexKey,
      id,
      compressorName,
      compressedData
    ))
    return node
  }

  disconnect (): void {
    this.redis.disconnect()
  }

  async findEdges (edge: SubjectEdgeSearch | ObjectEdgeSearch): Promise<ReadonlyArray<Edge>> {
    const { predicate, offset = 0, limit = 10 } = edge
    const { subject } = edge as SubjectEdgeSearch
    const { object } = edge as ObjectEdgeSearch
    const key = subject
      ? `${this.config.edgePrefix}:s:${subject}:${predicate}`
      : `${this.config.edgePrefix}:o:${object}:${predicate}`
    const idWeightPairs = await this.redis.zrangebyscore(
      key,
      -Infinity,
      Infinity,
      'WITHSCORES',
      'LIMIT',
      `${offset}`,
      `${limit}`
    )

    const edges: Edge[] = []
    for (let i = 0; i < idWeightPairs.length; i += 2) {
      const nodeId = Number(idWeightPairs[i])
      const weight = Number(idWeightPairs[i + 1])
      edges.push(subject ? {
        subject,
        predicate,
        object: nodeId,
        weight
      } : {
        subject: nodeId,
        predicate,
        object,
        weight
      })
    }
    return edges
  }

  async findNode (id: number): Promise<Node|null> {
    const nodeKey = this.nodeKey(id)
    const [compressorName, compressedData] = await this.redis.hmgetBuffer(nodeKey, 'c', 'data')
    if (!compressedData || !compressorName) {
      return null
    }
    const data = await decompressData({
      name: compressorName.toString(),
      data: compressedData
    }, this.config.compressors)
    return this.messagePack.decode(data)
  }

  async nodeExists (id: number): Promise<boolean> {
    return Boolean(await this.redis.exists(this.nodeKey(id)))
  }

  async putNode (node: Node): Promise<Node> {
    const { id } = node
    const data = this.messagePack.encode(node).slice()
    const { name: compressorName, data: compressedData } = await compressData(data, this.config.compressors)
    await decodeError(this.redis.putNode(this.nodeKey(id), id, compressorName, compressedData))
    return node
  }

  async updateNode (node: Node): Promise<Node> {
    const { id } = node
    const oldNode = await this.findNode(id)
    if (!oldNode) {
      throw new Error(`Node:${id} doesn't exist cannot update`)
    }
    const updatedNode = {
      ...oldNode,
      ...node
    }
    return this.putNode(updatedNode)
  }

  private evalCommands (): void {
    this.evalCreateEdge()
    this.evalCreateNode()
    this.evalPutNode()
  }

  private evalCreateEdge (): void {
    this.redis.defineCommand('createEdge', {
      numberOfKeys: 4,
      lua: `
        local objectEdgeKey = KEYS[1]
        local subjectEdgeKey = KEYS[2]
        local subjectKey = KEYS[3]
        local objectKey = KEYS[4]

        local subjectId = ARGV[1]
        local objectId = ARGV[2]
        local weight = ARGV[3]

        if redis.call("exists", subjectKey) == 0 then
          error('λsubject:' .. subjectId .. ' does not exist at key "' .. subjectKey .. '"λ')
        end

        if redis.call("exists", objectKey) == 0 then
          error('λobject:' .. objectId .. ' does not exist at key "' .. objectKey .. '"λ')
        end

        redis.call('zadd', objectEdgeKey, weight, subjectId)
        redis.call('zadd', subjectEdgeKey, weight, objectId)
      `
    })
  }

  private evalCreateNode (): void {
    this.redis.defineCommand('createNode', {
      numberOfKeys: 2,
      lua: `
        local nodeKey = KEYS[1]
        local nodeIndexKey = KEYS[2]
        local id = ARGV[1]
        local compressor = ARGV[2]
        local data = ARGV[3]

        if redis.call("exists", nodeKey) == 1 then
          error('λnode:' .. id .. ' already exist at key "' .. nodeKey .. '"λ')
        end

        redis.call('hmset', nodeKey, 'id', id, 'c', compressor, 'data', data)
        redis.call('zadd', nodeIndexKey, '0', id)
      `
    })
  }

  private evalPutNode (): void {
    this.redis.defineCommand('putNode', {
      numberOfKeys: 1,
      lua: `
        local nodeKey = KEYS[1]
        local id = ARGV[1]
        local compressor = ARGV[2]
        local data = ARGV[3]

        if redis.call("exists", nodeKey) == 0 then
          error('λnode:' .. id .. ' does not exist at key "' .. nodeKey .. '"λ')
        end

        redis.call('hmset', nodeKey, 'id', id, 'c', compressor, 'data', data)
      `
    })
  }

  private async getNextId (): Promise<number > {
    return this.redis.incr(this.config.guidKey)
  }

  private nodeKey (id): string {
    return `${this.config.nodeKeyPrefix}${id}`
  }
}
