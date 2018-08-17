import { compress, decompress } from 'iltorb'
import { Compressor } from './types'

// https://github.com/DefinitelyTyped/DefinitelyTyped/pull/28194
declare module 'iltorb' {
  function compress (buffer: Buffer, options?: BrotliEncodeParams): Promise<Buffer>
  function decompress (buffer: Buffer): Promise<Buffer>
}

export const brotliCompression = {
  name: 'brotli',
  compress (data: Buffer): Promise<Buffer> {
    return compress(data)
  },
  decompress (compressedData: Buffer): Promise<Buffer> {
    return decompress(compressedData)
  }
} as Compressor
