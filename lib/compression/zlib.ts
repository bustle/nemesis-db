import { promisify } from 'util'
import { gunzip, gzip } from 'zlib'
import { Compressor } from './types'

const gzipAsync = promisify(gzip) as (Buffer) => Promise<Buffer>
const gunzipAsync = promisify(gunzip) as (Buffer) => Promise<Buffer>

export const zlibCompression = {
  name: 'zlib',
  compress (data: Buffer): Promise<Buffer> | null {
    return gzipAsync(data)
  },
  decompress (compressedData: Buffer): Promise<Buffer> {
    return gunzipAsync(compressedData)
  }
} as Compressor
