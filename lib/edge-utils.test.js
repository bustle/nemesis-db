import { interfaceDefs, nodeDefs, edgeDefs } from './test/graph'
import { buildSchema } from './schema'
import { isObjectOf, isSubjectOf } from './edge-utils'

describe('Edge Definitions', () => {
  let schema, HasCarPoolBuddy, HasDriver
  before(() => {
    schema = buildSchema({ interfaceDefs, nodeDefs, edgeDefs })
    HasCarPoolBuddy = schema.registry.HasCarPoolBuddy
    HasDriver = schema.registry.HasDriver
  })
  it('can tell if a node is a valid Subject', () => {
    assert.equal(isSubjectOf(HasCarPoolBuddy, { _nodeType: 'Passenger', id: 99999999 }), true)
    assert.equal(isSubjectOf(HasCarPoolBuddy, { _nodeType: 'Foo', id: 99999999 }), false)
    assert.equal(isSubjectOf(HasDriver, { _nodeType: 'Car', id: 99999999 }), true)
    assert.equal(isSubjectOf(HasDriver, { _nodeType: 'Foo', id: 99999999 }), false)
  })
  it('can tell if a node is a valid Object', () => {
    assert.equal(isObjectOf(HasCarPoolBuddy, { _nodeType: 'Passenger', id: 99999999 }), true)
    assert.equal(isObjectOf(HasCarPoolBuddy, { _nodeType: 'Foo', id: 99999999 }), false)
  })
})
