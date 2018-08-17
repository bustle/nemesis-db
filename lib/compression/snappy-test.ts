import * as chai from 'chai'
import * as chaiAsPromised from 'chai-as-promised'
import { snappyCompression } from './snappy'

chai.use(chaiAsPromised)
const assert = chai.assert

describe('snappyCompression', () => {
  it('has a good name', () => {
    assert.equal(snappyCompression.name, 'snappy')
  })
  it('compresses and decompresses', async () => {
    const data = Buffer.from('all star')
    const compressedData = await snappyCompression.compress(data)
    assert.deepEqual(await snappyCompression.decompress(compressedData), data)
  })
  it('throws on bad compressed data', async () => {
    await assert.isRejected(Promise.resolve(snappyCompression.decompress(Buffer.from('foobar'))))
  })
})
