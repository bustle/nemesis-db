// custom graph used only for testing the graph library itself
import { ONE_TO_ONE, MANY_TO_MANY } from '../multiplicities'
import { GraphLoader } from '../index'
import { EdgeDefinition, NodeDefinition, InterfaceDefinition } from '../definitions'
import { redis } from 'app/globals'

export const Vehicle = new InterfaceDefinition({
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

export const MatchBoxRacer = new InterfaceDefinition({
  name: 'MatchBoxRacer',
  fields: {
    make: {
      nullable: true,
      type: 'string',
    },
  },
})

export const InvalidMatchBoxCar = new NodeDefinition({
  name: 'InvalidMatchBoxCar',
  interfaces: ['Vehicle', 'MatchBoxRacer'],
  fields: {},
})

export const Car = new NodeDefinition({
  name: 'Car',
  interfaces: ['Vehicle'],
  fields: {
    isCompact: {
      type: 'boolean',
      nullable: true,
    },
    metadata: {
      nullable: true,
      type: 'JSON',
    },
    isCrashed: {
      type: 'boolean',
      defaultValue: false,
      missingFieldValue: false,
    },
    passedInspection: {
      type: 'boolean',
      nullable: true,
      defaultValue: false,
    },
    isRegistered: {
      type: 'boolean',
      nullable: true,
      defaultValue: null,
    },
  },
})

const Van = new NodeDefinition({
  name: 'Van',
  interfaces: ['Vehicle'],
  fields: {
    marked: {
      type: 'boolean',
      missingFieldValue: false,
    },
  },
})

export const Passenger = new NodeDefinition({
  name: 'Passenger',
  fields: {
    name: {
      type: 'string',
      index: true,
      missingFieldValue: '',
    },
    nickname: {
      type: 'string',
      index: true,
      nullable: true,
    },
  },
})

const CarModel = new NodeDefinition({
  name: 'CarModel',
  fields: {
    name: {
      type: 'string',
      index: true,
      missingFieldValue: '',
    },
  },
})

const User = new NodeDefinition({
  name: 'User',
  fields: {
    name: {
      type: 'string',
      missingFieldValue: '',
    },
    loginAttempts: {
      type: 'JSON',
      missingFieldValue: [],
      defaultValue: [],
    },
  },
})

export const HasDriver = new EdgeDefinition({
  name: 'HasDriver',
  subjects: ['Vehicle'],
  objects: ['Passenger'],
  multiplicity: ONE_TO_ONE,
})

export const HasCarPoolBuddy = new EdgeDefinition({
  name: 'HasCarPoolBuddy',
  subjects: ['Passenger'],
  objects: ['Passenger'],
  multiplicity: MANY_TO_MANY,
})

export const SerialNumber = new EdgeDefinition({
  name: 'SerialNumber',
  subjects: ['CarModel'],
  objects: ['Car'],
  multiplicity: ONE_TO_ONE,
  dependentDestroy: true,
})

export const RegistrationNumber = new EdgeDefinition({
  name: 'RegistrationNumber',
  subjects: ['Passenger'],
  objects: ['Car'],
  multiplicity: ONE_TO_ONE,
  dependentDestroy: true,
  fields: {
    registration: {
      type: 'string',
      missingFieldValue: '',
    },
  },
})

export const interfaceDefs = { Vehicle }
export const nodeDefs = { Car, Passenger, Van, CarModel, User }
export const edgeDefs = { HasDriver, HasCarPoolBuddy, SerialNumber, RegistrationNumber }

export function testConnectGraph() {
  return new GraphLoader({
    redis,
    nodeDefs,
    edgeDefs,
    interfaceDefs,
  })
}
