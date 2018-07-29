import * as invariant from 'invariant'
import * as Redis from 'ioredis'
import * as messagePack from 'msgpack5'

export interface Node {
  readonly id: number
  [key: string]: any
}

export interface GraphConfigInput {
  guidKey?: string
  nodeKeyPrefix?: string
  edgePrefix?: string
}

export interface GraphConfig {
  readonly guidKey: string
  readonly nodeKeyPrefix: string
  readonly edgePrefix: string
}

export interface EdgeInput {
  object: number
  predicate: string
  subject: number
  weight?: number
}

export interface Edge {
  object: number
  predicate: string
  subject: number
  weight: number
}

export interface SubjectEdgeSearch {
  subject: number
  predicate: string
  offset?: number
  limit?: number
}

export interface ObjectEdgeSearch {
  object: number
  predicate: string
  offset?: number
  limit?: number
}

export class Graph {
  readonly config: GraphConfig
  readonly redis: Redis.Redis
  readonly messagePack: messagePack.MessagePack

  constructor(redisUrl: string, config?: GraphConfigInput) {
    this.config = {
      guidKey: 'counter:guid',
      nodeKeyPrefix: 'node:',
      edgePrefix: 'edge:',
      ...config
    }
    this.redis = new Redis(redisUrl)
    this.messagePack = messagePack()
  }

  disconnect() {
    this.redis.disconnect()
  }

  private async getNextId () {
    return this.redis.incr(this.config.guidKey)
  }

  private nodeKey(id) {
    return `${this.config.nodeKeyPrefix}${id}`
  }

  async nodeExists(id: number): Promise<boolean> {
    return !! (await this.redis.exists(this.nodeKey(id)))
  }

  async createNode(attributes): Promise<Node> {
    invariant(!attributes.id, `attributes already has an "id" property this is probably ok but I'm going ot panic anyway`)

    const id = await this.getNextId()
    invariant(!await this.nodeExists(id), `Node with id ${id} already exists unable to create node. Something very bad has just happened`)

    const node = {
      ...attributes,
      id
    }

    await this.redis.hmset(this.nodeKey(id), {
      id,
      data: this.messagePack.encode(node),
    })

    return node
  }

  async updateNode(node: Node): Promise<Node> {
    const { id } = node
    const oldNode = await this.findNode(id)
    invariant(oldNode, `Node:${id} doesn't exist cannot update`)
    const updatedNode = {
      ...oldNode,
      ...node
    }
    await this.redis.hmset(this.nodeKey(id), {
      id,
      data: this.messagePack.encode(updatedNode),
    })
    return updatedNode
  }

  async findNode(id: number): Promise<Node|null> {
    const nodeKey = `${this.config.nodeKeyPrefix}${id}`
    const data = await (this.redis as any).hgetBuffer(nodeKey, 'data')
    if (!data) {
      return null
    }
    return this.messagePack.decode(data)
  }

  async createEdge({ subject, predicate, object, weight = 0 }: EdgeInput): Promise<Edge> {
    invariant(await this.nodeExists(subject), `subject:${subject} does not exist`)
    invariant(await this.nodeExists(object), `object:${object} does not exist`)

    await Promise.all([
      this.redis.zadd(`${this.config.edgePrefix}:o:${object}:${predicate}`, `${weight}`, `${subject}`),
      this.redis.zadd(`${this.config.edgePrefix}:s:${subject}:${predicate}`, `${weight}`, `${object}`),
    ])
    return { subject, predicate, object, weight }
  }

  async findEdges(edge: SubjectEdgeSearch | ObjectEdgeSearch) {
    const { predicate, offset = 0, limit = 10 } = edge
    const { subject } = edge as SubjectEdgeSearch
    const { object } = edge as ObjectEdgeSearch
    const key = subject ? `${this.config.edgePrefix}:s:${subject}:${predicate}` : `${this.config.edgePrefix}:o:${object}:${predicate}`
    const idWeightPairs = await this.redis.zrangebyscore(key, -Infinity, Infinity, 'WITHSCORES', 'LIMIT', `${offset}`, `${limit}`)

    const edges: Array<Edge> = []
    for (let i = 0; i < idWeightPairs.length; i += 2) {
      const nodeId = Number(idWeightPairs[i])
      const weight = Number(idWeightPairs[i + 1])
      edges.push(subject ? {
        subject,
        predicate,
        object: nodeId,
        weight
      }: {
        subject: nodeId,
        predicate,
        object,
        weight
      })
    }
    return edges
  }
}
