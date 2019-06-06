import chaiSubset from 'chai-subset'
import chai, { expect, assert } from 'chai'
import chaiAsPromised from 'chai-as-promised'

chai.use(chaiAsPromised)
chai.use(chaiSubset)

global.expect = expect
global.assert = assert
global.invariant = require('invariant')
