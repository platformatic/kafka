import { type AclOperation, type AclPermissionType, type PatternType, type ResourceType } from './enumerations.ts'

type AclFilterOptionals = 'resourceName' | 'principal' | 'host'

export interface AclTarget {
  resourceType: ResourceType
  resourceName: string
  patternType: PatternType
}

export interface AclPermission {
  principal: string
  host: string
  operation: AclOperation
  permissionType: AclPermissionType
}

export type Acl = AclTarget & AclPermission

export type AclFilter = { [K in AclFilterOptionals]: Acl[K] | null | undefined } & Omit<Acl, AclFilterOptionals>
