import * as chai from 'chai'
import * as chaiAsPromised from 'chai-as-promised'
import { brotliCompression } from './brotli'

chai.use(chaiAsPromised)
const assert = chai.assert

describe('brotliCompression', () => {
  it('has a good name', () => {
    assert.equal(brotliCompression.name, 'brotli')
  })
  it('compresses and decompresses', async () => {
    const data = Buffer.from('all star')
    const compressedData = await brotliCompression.compress(data)
    assert.deepEqual(await brotliCompression.decompress(compressedData), data)
  })
  it('throws on bad compressed data', async () => {
    await assert.isRejected(Promise.resolve(brotliCompression.decompress(Buffer.from('foobar'))))
  })
})
