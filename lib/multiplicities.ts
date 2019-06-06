export const ONE_TO_ONE = 'ONE_TO_ONE'
export const ONE_TO_MANY = 'ONE_TO_MANY'
export const MANY_TO_ONE = 'MANY_TO_ONE'
export const MANY_TO_MANY = 'MANY_TO_MANY'

export const validDependentMultiplicities: ReadonlyArray<ValidDependentMultiplicity> = [
  ONE_TO_ONE,
  ONE_TO_MANY,
  MANY_TO_ONE,
]
export type ValidDependentMultiplicity = 'ONE_TO_ONE' | 'ONE_TO_MANY' | 'MANY_TO_ONE'
export type Multiplicity = ValidDependentMultiplicity | 'MANY_TO_MANY'

export default [ONE_TO_ONE, ONE_TO_MANY, MANY_TO_ONE, MANY_TO_MANY]
