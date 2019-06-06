import { buildSchema } from './schema'
import {
  nodeDefs,
  interfaceDefs,
  edgeDefs,
  Vehicle,
  Car,
  HasDriver,
  MatchBoxRacer,
  InvalidMatchBoxCar,
} from './test/graph'

describe('#buildSchema', () => {
  describe('Interface Definitions', () => {
    it('can build the interface defintions', () => {
      assert.isOk(buildSchema({ interfaceDefs }))
    })
    it('cannot build with duplicate interface defintions', () => {
      assert.throws(
        () => buildSchema({ interfaceDefs: { Vehicle, OtherVehicle: Vehicle } }),
        /Schema already contains definition/
      )
    })
  })

  describe('Node Definitions', () => {
    it('can build the node defintions', () => {
      assert.isOk(buildSchema({ interfaceDefs, nodeDefs }))
    })

    it("every definition inherit's Node's fields by default", () => {
      const schema = buildSchema({ interfaceDefs, nodeDefs })
      const car = schema.registry.Car
      assert.isOk(car.fields.id)
      assert.isOk(car.fields.createdAt)
      assert.isOk(car.fields.updatedAt)
    })

    it('cannot build with duplicate node defintions', () => {
      assert.throws(
        () => buildSchema({ interfaceDefs, nodeDefs: { Car, OtherCar: Car } }),
        /Schema already contains definition/
      )
    })

    it('throws if missing needed interface defintions', () => {
      assert.throws(() => buildSchema({ nodeDefs }), /Type \w+ has unknown interface/)
    })

    it('throws if a node has interfaces that have incompatible fields', () => {
      assert.throws(
        () => buildSchema({ interfaceDefs: { Vehicle, MatchBoxRacer }, nodeDefs: { InvalidMatchBoxCar } }),
        /InvalidMatchBoxCar implements MatchBoxRacer and Vehicle which have incompatible field definitions for "make"/
      )
    })
  })

  describe('Edge Definitions', () => {
    it('can build the node defintions', () => {
      assert.isOk(buildSchema({ interfaceDefs, nodeDefs, edgeDefs }))
    })
    it('cannot build with duplicate edge defintions', () => {
      assert.throws(
        () => buildSchema({ interfaceDefs, nodeDefs, edgeDefs: { HasDriver, OtherHasDriver: HasDriver } }),
        /Schema already contains definition/
      )
    })
    it('throws if missing needed interface defintions', () => {
      assert.throws(() => buildSchema({ nodeDefs, edgeDefs }), /Type \w+ has unknown interface/)
    })
    it('throws if missing needed node defintions', () => {
      assert.throws(() => buildSchema({ interfaceDefs, edgeDefs }), /Object type: "\w+" is does not exist/)
    })
    it('throws on invalid edge defintions', () => {
      assert.throws(
        () => buildSchema({ edgeDefs: { foo: {} } }),
        /typeDef with name: "\w+" is not an instance of EdgeDefinition/
      )
    })
  })
})
