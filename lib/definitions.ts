import invariant from 'invariant'
import { isEmpty } from 'lodash'
import multiplicities, {
  validDependentMultiplicities,
  Multiplicity,
  ValidDependentMultiplicity,
} from './multiplicities'
import { serializeField } from './node-utils'

export type FieldType = 'string' | 'number' | 'JSON' | 'boolean' | 'ID'
const FIELD_TYPES: ReadonlyArray<FieldType> = ['string', 'number', 'JSON', 'boolean', 'ID']

const RESERVERED_PROPERTIES: ReadonlyArray<ReservedProperty> = ['id', 'createdAt', 'updatedAt']
type ReservedProperty = 'id' | 'createdAt' | 'updatedAt'

function configureFieldDef(defName: string, fieldDef: FieldDefinition, fieldName: string) {
  invariant(
    FIELD_TYPES.includes(fieldDef.type),
    `Type "${defName}" has unknown field type ${fieldName}: ${fieldDef.type} must be one of ${FIELD_TYPES}`
  )
  invariant(
    !RESERVERED_PROPERTIES.includes(fieldName as ReservedProperty),
    `${defName} cannot use reserved "fieldName": ${fieldName}`
  )
  const missingFieldValue = fieldDef.missingFieldValue
  if (fieldDef.defaultValue !== undefined) {
    try {
      fieldDef.defaultValue = serializeField(fieldDef, fieldDef.defaultValue)
    } catch (error) {
      throw new Error(`Field "${defName}:${fieldName}" has an invalid default value: ${error.message}`)
    }
  }

  if (fieldDef.nullable) {
    if (fieldDef.missingFieldValue !== undefined) {
      throw new Error(`Field "${defName}:${fieldName} is nullable so can't have a missingFieldValue (defaults to null)`)
    }
    try {
      fieldDef.missingFieldValue = serializeField(fieldDef, missingFieldValue === undefined ? null : missingFieldValue)
    } catch (error) {
      throw new Error(`Field "${defName}:${fieldName}" has an invalid missing field value: ${error.message}`)
    }
  } else {
    if (missingFieldValue === undefined) {
      throw new Error(`Field "${defName}:${fieldName}" is not nullable and has no missing field value`)
    }
    try {
      fieldDef.missingFieldValue = serializeField(fieldDef, missingFieldValue)
    } catch (error) {
      throw new Error(`Field "${defName}:${fieldName}" has an invalid missing field value: ${error.message}`)
    }
  }
}

interface EdgeDefinitionOptions {
  readonly dependentDestroy?: boolean
  readonly fields?: FieldDefMap
  readonly multiplicity: Multiplicity | ValidDependentMultiplicity
  readonly name: string
  readonly objects?: ReadonlyArray<any>
  readonly subjects?: ReadonlyArray<any>
}

export class EdgeDefinition {
  get type(): 'edge' {
    return 'edge'
  }
  readonly dependentDestroy: boolean
  readonly fields?: FieldDefMap
  readonly multiplicity: Multiplicity | ValidDependentMultiplicity
  readonly name: string
  objects: ReadonlyArray<any>
  subjects: ReadonlyArray<any>
  constructor({
    name,
    multiplicity,
    objects = [],
    subjects = [],
    dependentDestroy = false,
    fields = {},
  }: EdgeDefinitionOptions) {
    invariant(name, 'Edge definitions require "name" field')
    invariant(multiplicities.includes(multiplicity), `value "${multiplicity}" is an invalid multiplicity`)
    invariant(
      Array.isArray(objects) && objects.length,
      'Edge definitions require "objects" field to be an array of at least one type'
    )
    invariant(
      Array.isArray(subjects) && subjects.length,
      'Edge definitions require "subjects" field to be an array of at least one type'
    )
    invariant(typeof dependentDestroy === 'boolean', `Edge Defintion "dependentDestory" field must be a boolean`)
    if (dependentDestroy) {
      invariant(
        validDependentMultiplicities.includes(multiplicity as ValidDependentMultiplicity),
        `"dependentDestroy" can only be used with one of the following Multiplicities: ${validDependentMultiplicities.join(
          ', '
        )}`
      )
    }
    Object.keys(fields).forEach(fieldName => configureFieldDef(name, fields[fieldName], fieldName))
    this.name = name
    this.multiplicity = multiplicity
    this.fields = isEmpty(fields) ? undefined : fields
    this.objects = objects
    this.subjects = subjects
    this.dependentDestroy = dependentDestroy
  }
  toString() {
    return this.name
  }
}

export class NodeDefinition {
  fields: FieldDefMap
  interfaces: Array<string | InterfaceDefinition>
  readonly name: string

  get type(): 'node' {
    return 'node'
  }

  constructor({
    name,
    fields = {},
    interfaces = [],
  }: {
    readonly fields?: FieldDefMap
    readonly interfaces?: any[]
    readonly name: string
  }) {
    invariant(name, `a NodeDefinition must have a "name" property`)
    invariant(Array.isArray(interfaces), `"interfaces" must be an array`)
    Object.keys(fields).forEach(fieldName => configureFieldDef(name, fields[fieldName], fieldName))
    this.name = name
    this.fields = fields
    this.interfaces = interfaces[0] === 'Node' ? interfaces : ['Node', ...interfaces]
  }

  toString() {
    return this.name
  }
}

export interface FieldDefinition {
  readonly index?: boolean
  missingFieldValue?: any
  readonly nullable?: boolean
  readonly type: FieldType
  defaultValue?: any
  /**
   * Dear confused developer, this is used when auto generating TS types for this field, it must exist in "graphdb/user-types.d.ts"
   */
  readonly tsType?: string
}

export interface FieldDefMap {
  readonly [key: string]: FieldDefinition
}

export class InterfaceDefinition {
  get type(): 'interface' {
    return 'interface'
  }
  readonly fields: FieldDefMap
  readonly implementerNames: any[]
  readonly implementers: any[]
  readonly name: string
  constructor({ name, fields = {} }: { readonly fields?: FieldDefMap; readonly name: string }) {
    invariant(name, `an InterfaceDefinition must have a "name" property`)
    if (name !== 'Node') {
      Object.keys(fields).forEach(fieldName => configureFieldDef(name, fields[fieldName], fieldName))
    }
    this.name = name
    this.fields = fields
    this.implementers = []
    this.implementerNames = []
  }
  toString() {
    return this.name
  }
}

export const Node = new InterfaceDefinition({
  name: 'Node',
  fields: {
    id: {
      type: 'ID',
      missingFieldValue: 0,
    },
    createdAt: {
      type: 'number',
      missingFieldValue: 0,
    },
    updatedAt: {
      type: 'number',
      missingFieldValue: 0,
    },
  },
})
