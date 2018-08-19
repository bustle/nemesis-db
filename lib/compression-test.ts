import * as chai from 'chai'
import * as chaiAsPromised from 'chai-as-promised'
import { compressData, decompressData } from './compression'
import { brotliCompression } from './compression/brotli'
import { snappyCompression } from './compression/snappy'
import { zlibCompression } from './compression/zlib'

chai.use(chaiAsPromised)
const assert = chai.assert

const smallData = Buffer.from('Hey')
const largeData = Buffer.alloc(1024 * 1024)

const compressors = new Map()
compressors.set(brotliCompression.name, brotliCompression)
compressors.set(snappyCompression.name, snappyCompression)
compressors.set(zlibCompression.name, zlibCompression)

describe('compression', () => {
  describe('compressData', () => {
    it('compresses small data', async () => {
      const compressedData = await compressData(smallData, compressors)
      assert.isAtMost(compressedData.data.length, smallData.length)
    })
    it('compresses large data', async () => {
      const compressedData = await compressData(largeData, compressors)
      assert.isAtMost(compressedData.data.length, largeData.length)
    })
  })

  describe('decompressData', () => {
    it('decompresses small data', async () => {
      const compressedData = await compressData(smallData, compressors)
      const data = await decompressData(compressedData, compressors)
      assert.deepEqual(data, smallData)
    })
    it('decompresses large data', async () => {
      const compressedData = await compressData(largeData, compressors)
      const data = await decompressData(compressedData, compressors)
      assert.deepEqual(data, largeData)
    })
    it(`throws if it doesn't know the compressor`, async () => {
      const compressedData = await compressData(largeData, compressors)
      await assert.isRejected(decompressData(compressedData, new Map()))
    })
  })
})
