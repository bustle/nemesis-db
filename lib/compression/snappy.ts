import { compress, uncompress } from 'snappy'
import { promisify } from 'util'
import { Compressor } from './types'

const compressAsync = promisify(compress) as (Buffer) => Promise<Buffer>
const uncompressAsync = promisify(uncompress) as (Buffer) => Promise<Buffer>

const GZIP_HEADER_SIZE = 18

export const snappyCompression = {
  name: 'snappy',
  compress (data: Buffer): Promise<Buffer> | null {
    return compressAsync(data)
  },
  decompress (compressedData: Buffer): Promise<Buffer> {
    return uncompressAsync(compressedData)
  }
} as Compressor
