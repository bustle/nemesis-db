
export interface Compressor {
  readonly compress: (Buffer) => Buffer | Promise<Buffer>
  readonly decompress: (Buffer) => Buffer | Promise<Buffer>
  readonly name: string
}

export interface CompressedData {
  readonly data: Buffer
  readonly name: string
}
