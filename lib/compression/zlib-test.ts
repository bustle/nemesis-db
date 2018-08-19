import * as chai from 'chai'
import * as chaiAsPromised from 'chai-as-promised'
import { zlibCompression } from './zlib'

chai.use(chaiAsPromised)
const assert = chai.assert

describe('zlibCompression', () => {
  it('has a good name', () => {
    assert.equal(zlibCompression.name, 'zlib')
  })
  it('compresses and decompresses', async () => {
    const data = Buffer.from('all star')
    const compressedData = await zlibCompression.compress(data)
    assert.deepEqual(await zlibCompression.decompress(compressedData), data)
  })
  it('throws on bad compressed data', async () => {
    await assert.isRejected(Promise.resolve(zlibCompression.decompress(Buffer.from('foobar'))))
  })
})
