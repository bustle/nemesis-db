import invariant from 'invariant'
import { flatten } from 'lodash'
import { hasInstanceOf, IdObject, TypedIdObject, GUID } from './node-utils'
import { EdgeDefinition, NodeDefinition, InterfaceDefinition } from './definitions'
import { GraphDBSchema, EdgeDefs } from './schema'

export const MAX_ID_SIZE = 2 ** 32

/**
 * checks if a node has a valid id
 */
export function hasValidId({ id }: IdObject): boolean {
  const numId = Number(id)
  return Boolean(numId && numId < MAX_ID_SIZE && numId > 0)
}

/**
 * convert a node's id to a 32 bit int buffer
 */
export function createIdBuffer({ id }: IdObject): Buffer {
  invariant(hasValidId({ id }), `Node with id ${id} is out of range.`)
  const buffer = Buffer.alloc(4)
  buffer.writeUInt32BE(Number(id), 0)
  return buffer
}

/**
 * convert a node's 32 bit int buffer to an id
 */
export const readIdBuffer = (buffer: Buffer): GUID => String(buffer.readUInt32BE(0))

/**
 * check if a node is the `object` of an edge definition
 */
export function isObjectOf(edgeDef: EdgeDefinition, node: TypedIdObject) {
  return !!edgeDef.objects.find(object => hasInstanceOf(object, node))
}

/**
 * check if a node is the `subject` of an edge definition
 */
export function isSubjectOf(edgeDef: EdgeDefinition, node: TypedIdObject) {
  return !!edgeDef.subjects.find(subject => hasInstanceOf(subject, node))
}

export function isObjectOfInvariant(edgeDef: EdgeDefinition, object: TypedIdObject) {
  invariant(
    isObjectOf(edgeDef, object),
    `Object with nodeType: ${object._nodeType} is invalid for predicate "${edgeDef.name}"`
  )
}

export function isSubjectOfInvariant(edgeDef: EdgeDefinition, subject: TypedIdObject) {
  invariant(
    isSubjectOf(edgeDef, subject),
    `Subject with nodeType: ${subject._nodeType} is invalid for predicate "${edgeDef.name}"`
  )
}

export function subjectEdgeDefs(edgeDefs: EdgeDefs, node: TypedIdObject) {
  const edgeDefinitions: EdgeDefinition[] = []
  for (const edgeDef of Object.values(edgeDefs)) {
    if (isSubjectOf(edgeDef, node)) {
      edgeDefinitions.push(edgeDef)
    }
  }
  return edgeDefinitions
}

export function objectEdgeDefs(edgeDefs: EdgeDefs, node: TypedIdObject) {
  const edgeDefinitions: EdgeDefinition[] = []
  for (const edgeDef of Object.values(edgeDefs)) {
    if (isObjectOf(edgeDef, node)) {
      edgeDefinitions.push(edgeDef)
    }
  }
  return edgeDefinitions
}

/**
 * returns the dependent edge definitions of a node defintion
 */
export function findDependentEdgeDefs({ dependentEdges }: GraphDBSchema, node: NodeDefinition) {
  const { interfaces, name: nodeDefName } = node
  return [
    ...dependentEdges[nodeDefName],
    ...flatten((interfaces as InterfaceDefinition[]).map(({ name }) => dependentEdges[name])),
  ]
}
