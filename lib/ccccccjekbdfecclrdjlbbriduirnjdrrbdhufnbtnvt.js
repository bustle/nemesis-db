/* eslint-disable no-new, no-unused-expression */
import { EdgeDefinition, NodeDefinition, InterfaceDefinition } from './definitions'
import { ONE_TO_ONE, MANY_TO_MANY } from './multiplicities'

describe('Type Definitions', () => {
  describe('EdgeDefinition', () => {
    it('can build the edge defintions', () => {
      assert.isOk(
        new EdgeDefinition({
          name: 'HasDriver',
          subjects: ['Vehicle'],
          objects: ['Passenger'],
          multiplicity: ONE_TO_ONE,
        })
      )
    })
    it('throws on invalid edge defintions', () => {
      assert.throws(() => {
        new EdgeDefinition({
          name: 'RegistrationNumber',
          subjects: ['Passenger'],
          objects: ['Car'],
          multiplicity: MANY_TO_MANY,
          dependentDestroy: true,
        })
      }, /"dependentDestroy" can only be used with one of the following Multiplicities/)
      assert.throws(() => {
        new EdgeDefinition({
          subjects: ['Vehicle'],
          objects: ['Passenger'],
          multiplicity: ONE_TO_ONE,
        })
      }, /Edge definitions require "name" field/)
      assert.throws(() => {
        new EdgeDefinition({
          name: 'HasDriver',
          objects: ['Passenger'],
          multiplicity: ONE_TO_ONE,
        })
      }, /Edge definitions require "subjects" field to be an array of at least one type/)
      assert.throws(() => {
        new EdgeDefinition({
          name: 'HasDriver',
          subjects: ['Vehicle'],
          multiplicity: ONE_TO_ONE,
        })
      }, /Edge definitions require "objects" field to be an array of at least one type/)
      assert.throws(() => {
        new EdgeDefinition({
          name: 'HasDriver',
          subjects: ['Vehicle'],
          objects: ['Passenger'],
        })
      }, /value "undefined" is an invalid multiplicity/)
    })
  })
  describe('NodeDefinition', () => {
    it('can build the node defintions', () => {
      assert.isOk(
        new NodeDefinition({
          name: 'Car',
          interfaces: ['Node', 'Vehicle'],
          fields: {
            isCompact: {
              nullable: false,
              type: 'boolean',
              missingFieldValue: false,
            },
            metadata: {
              nullable: true,
              type: 'JSON',
            },
          },
        })
      )
    })
    it('every node definition interfaces with "Node" by default', () => {
      const car = new NodeDefinition({ name: 'Car' })
      assert.isOk(car.interfaces.includes('Node'))
    })
    it('throws on invalid edge defintions', () => {
      assert.throws(() => {
        new NodeDefinition({
          name: 'Car',
          interfaces: ['Vehicle'],
          fields: {
            isCompact: {
              nullable: false,
              type: 'boolean',
              missingFieldValue: 'false',
            },
          },
        })
      }, 'Field "Car:isCompact" has an invalid missing field value: invalid boolean')
      assert.throws(() => {
        new NodeDefinition({
          name: 'Car',
          interfaces: ['Vehicle'],
          fields: {
            id: {
              nullable: false,
              type: 'boolean',
              missingFieldValue: 'false',
            },
          },
        })
      }, /Car cannot use reserved "fieldName": id/)
      assert.throws(() => {
        new NodeDefinition({
          name: 'Car',
          interfaces: ['Vehicle'],
          fields: {
            metadata: {
              nullable: false,
              type: 'JSON',
            },
          },
        })
      }, /Field "Car:metadata" is not nullable and has no missing field value/)
      assert.throws(() => {
        new NodeDefinition({
          name: 'Car',
          interfaces: ['Vehicle'],
          fields: {
            metadata: {
              nullable: true,
              type: 'JASON',
            },
          },
        })
      }, /Type "Car" has unknown field type metadata: JASON/)
    })
  })
  describe('InterfaceDefinition', () => {
    it('can build the node defintions', () => {
      assert.isOk(
        new InterfaceDefinition({
          name: 'Vehicle',
          fields: {
            make: {
              type: 'string',
              index: true,
              missingFieldValue: '',
            },
            model: {
              type: 'string',
              index: true,
              missingFieldValue: '',
            },
            nextMaintenance: {
              nullable: true,
              index: true,
              type: 'number',
            },
          },
        })
      )
    })
    it('throws on invalid edge defintions', () => {
      assert.throws(() => {
        new InterfaceDefinition({
          name: 'Vehicle',
          fields: {
            make: {
              type: 'string',
              index: true,
              missingFieldValue: '',
            },
            model: {
              type: 'string',
              index: true,
              missingFieldValue: '',
            },
            nextMaintenance: {
              nullable: false,
              index: true,
              type: 'number',
            },
          },
        })
      }, /Field "Vehicle:nextMaintenance" is not nullable and has no missing field value/)
    })
  })
})
