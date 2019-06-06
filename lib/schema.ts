import invariant from 'invariant'
import { cloneDeep, values, keys, isEqual } from 'lodash'
import { Node, InterfaceDefinition, NodeDefinition, EdgeDefinition, FieldDefinition } from './definitions'

function cloneDefinition<T extends NodeDefinition | InterfaceDefinition | EdgeDefinition>(
  typeDef: T,
  name: string,
  defClass: any
): T {
  invariant(typeDef instanceof defClass, `typeDef with name: "${name}" is not an instance of ${defClass.name}`)
  return cloneDeep(typeDef)
}

const validateNodeDef = (def: NodeDefinition, interfaceDefs: InterfaceDefs) => {
  const nodefields: { [key: string]: [string, FieldDefinition] } = {}
  def.interfaces.forEach(interfaceName => {
    const iFace = interfaceDefs[interfaceName.toString()]
    invariant(iFace, `Type ${def.name} has unknown interface "${interfaceName}"`)
    for (const [fieldName, fieldDef] of Object.entries(iFace.fields)) {
      if (!nodefields[fieldName]) {
        nodefields[fieldName] = [iFace.name, fieldDef]
        continue
      }
      const [prevIfaceName, prevIfaceDef] = nodefields[fieldName]
      if (!isEqual(prevIfaceDef, fieldDef)) {
        throw new Error(
          `${def.name} implements ${
            iFace.name
          } and ${prevIfaceName} which have incompatible field definitions for "${fieldName}"`
        )
      }
    }
  })
}

const validateEdgeDef = (def: EdgeDefinition, interfaceDefs: InterfaceDefs, nodeDefs: NodeDefs) => {
  const { subjects, objects } = def
  objects.forEach(name => {
    const hasValidObject = interfaceDefs[name] || nodeDefs[name]
    invariant(hasValidObject, `Error in Predicate: "${def}". Object type: "${name}" is does not exist in the schema`)
  })
  subjects.forEach(name => {
    const hasValidSubject = interfaceDefs[name] || nodeDefs[name]
    invariant(hasValidSubject, `Error in Predicate: "${def}". Subject type: "${name}" is does not exist in the schema`)
  })
}

const validateSchema = ({ interfaceDefs, nodeDefs, edgeDefs }: GraphDBSchema) => {
  values(nodeDefs).forEach(def => validateNodeDef(def, interfaceDefs))
  values(edgeDefs).forEach(def => validateEdgeDef(def, interfaceDefs, nodeDefs))
}

const linkSchema = ({ interfaceDefs, nodeDefs, edgeDefs, registry, dependentEdges }: GraphDBSchema) => {
  values(interfaceDefs).forEach(interfaceDef => {
    // set up the dependentEdges object with a list of all the interfaces
    dependentEdges[interfaceDef.name] = []
  })
  values(nodeDefs).forEach(nodeDef => {
    // set up the dependentEdges object with a list of all the nodes
    dependentEdges[nodeDef.name] = []
    // replace the names of interfaces with the typeDefs, inherit their field defs
    let interfaceFields = {}
    nodeDef.interfaces = nodeDef.interfaces.map(interfaceName => {
      const iface = interfaceDefs[interfaceName.toString()]
      iface.implementers.push(nodeDef)
      iface.implementerNames.push(nodeDef.name)
      interfaceFields = {
        ...interfaceFields,
        ...iface.fields,
      }
      return iface
    })
    nodeDef.fields = {
      ...interfaceFields,
      ...nodeDef.fields,
    }
  })
  for (const edgeDef of Object.values(edgeDefs)) {
    // add dependent destory relationships to the dependentEdges object
    if (edgeDef.dependentDestroy) {
      edgeDef.subjects.forEach(type => {
        dependentEdges[type].push(edgeDef)
      })
    }
    // replace the names of objects and subject with the typeDefs
    edgeDef.objects = edgeDef.objects.map(obj => registry[obj])
    edgeDef.subjects = edgeDef.subjects.map(sub => registry[sub])
  }
}

export interface NodeDefs {
  [key: string]: NodeDefinition
}

export interface InterfaceDefs {
  [key: string]: InterfaceDefinition
}

export interface EdgeDefs {
  [key: string]: EdgeDefinition
}

interface DependentEdges {
  [key: string]: EdgeDefinition[]
}

interface SchemaRegistry {
  [key: string]: InterfaceDefinition | NodeDefinition | EdgeDefinition
}

export interface GraphDBSchema {
  readonly dependentEdges: DependentEdges
  readonly edgeDefs: EdgeDefs
  readonly interfaceDefs: InterfaceDefs
  readonly nodeDefs: NodeDefs
  readonly registry: SchemaRegistry
}

interface BuildSchemaOptions {
  readonly edgeDefs: EdgeDefs
  readonly interfaceDefs: InterfaceDefs
  readonly nodeDefs: NodeDefs
}

export const buildSchema = ({ interfaceDefs, nodeDefs, edgeDefs }: BuildSchemaOptions): GraphDBSchema => {
  const registry: SchemaRegistry = {}
  const schema: GraphDBSchema = {
    registry,
    interfaceDefs: {},
    nodeDefs: {},
    edgeDefs: {},
    dependentEdges: {},
  }
  schema.interfaceDefs.Node = registry.Node = cloneDefinition(Node, 'Node', InterfaceDefinition)
  keys(interfaceDefs).forEach((name: string) => {
    const type = interfaceDefs[name]
    invariant(!registry[type.name], `Schema already contains definition named "${type}"`)
    schema.interfaceDefs[type.name] = registry[type.name] = cloneDefinition(type, name, InterfaceDefinition)
  })
  keys(nodeDefs).forEach((name: string) => {
    const type = nodeDefs[name]
    invariant(!registry[type.name], `Schema already contains definition named "${type}"`)
    schema.nodeDefs[type.name] = registry[type.name] = cloneDefinition(type, name, NodeDefinition)
  })
  keys(edgeDefs).forEach((name: string) => {
    const type = edgeDefs[name]
    invariant(!registry[type.name], `Schema already contains definition named "${type}"`)
    schema.edgeDefs[type.name] = registry[type.name] = cloneDefinition(type, name, EdgeDefinition)
  })
  validateSchema(schema)
  linkSchema(schema)
  return schema
}
