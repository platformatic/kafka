import {
  type AclOperationValue,
  type AclPermissionTypeValue,
  type ResourcePatternTypeValue,
  type ResourceTypeValue
} from './enumerations.ts'

export interface AclTarget {
  resourceType: ResourceTypeValue
  resourceName: string
  resourcePatternType: ResourcePatternTypeValue
}

export interface AclPermission {
  principal: string
  host: string
  operation: AclOperationValue
  permissionType: AclPermissionTypeValue
}

export type Acl = AclTarget & AclPermission

export type AclFilter = { [K in 'resourceName' | 'principal' | 'host']: Acl[K] | null | undefined } & Omit<
  Acl,
  'resourceName' | 'principal' | 'host'
>
