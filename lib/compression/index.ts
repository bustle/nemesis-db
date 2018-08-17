import { CompressedData, Compressor } from './types'
export { brotliCompression } from './brotli'
export { snappyCompression } from './snappy'
export { zlibCompression } from './zlib'

export async function compressData (
  uncompressedData: Buffer,
  compressors: Map<string, Compressor>
): Promise<CompressedData> {
  const compressorObjects = [...compressors.values()]
  const compressedData = await Promise.all(compressorObjects.map(async ({ name, compress }) => ({
      name,
      data: await compress(uncompressedData)
    })
  ))

  return compressedData.reduce((smallest, next) => {
    // console.log('next', next.name, next.data.length, 'smallest', smallest.name, smallest.data.length)
    if (next.data.length < smallest.data.length) {
      return next
    }
    return smallest
  }, {
    name: '',
    data: uncompressedData
  })
}

export async function decompressData (
  compressedData: CompressedData,
  compressors: Map<string, Compressor>
): Promise<Buffer> {
  const { name, data } = compressedData
  if (name === '') {
    return compressedData.data
  }

  const compressor = compressors.get(name)
  if (!compressor) {
    throw new Error(`Unknown compressor ${name}`)
  }
  return compressor.decompress(data)
}
