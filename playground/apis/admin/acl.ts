import { api as createAclsV3 } from '../../../src/apis/admin/create-acls-v3.ts'
import { api as deleteAclsV3 } from '../../../src/apis/admin/delete-acls-v3.ts'
import { api as describeAclsV3 } from '../../../src/apis/admin/describe-acls-v3.ts'
import { AclOperations, AclPermissionTypes, PatternTypes, ResourceTypes } from '../../../src/apis/enumerations.ts'
import { Connection } from '../../../src/network/connection.ts'
import { performAPICallWithRetry } from '../../utils.ts'

const connection = new Connection('123')
await connection.connect('localhost', 9092)

await performAPICallWithRetry('CreateAcls', () =>
  createAclsV3.async(connection, [
    {
      resourceType: ResourceTypes.TOPIC,
      resourceName: 'temp',
      patternType: PatternTypes.LITERAL,
      principal: 'abc:cde',
      host: '*',
      operation: AclOperations.READ,
      permissionType: AclPermissionTypes.DENY
    }
  ]))

await performAPICallWithRetry('DescribeAcls', () =>
  describeAclsV3.async(connection, {
    resourceType: ResourceTypes.TOPIC,
    resourceName: 'temp',
    patternType: PatternTypes.LITERAL,
    principal: null,
    host: null,
    operation: AclOperations.READ,
    permissionType: AclPermissionTypes.DENY
  }))

await performAPICallWithRetry('DescribeAcls', () =>
  describeAclsV3.async(connection, {
    resourceType: ResourceTypes.TOPIC,
    resourceName: 'temp',
    patternType: PatternTypes.LITERAL,
    principal: null,
    host: null,
    operation: AclOperations.READ,
    permissionType: AclPermissionTypes.ALLOW
  }))

await performAPICallWithRetry('DeleteAcls', () =>
  deleteAclsV3.async(connection, [
    {
      resourceType: ResourceTypes.TOPIC,
      resourceName: 'temp',
      patternType: PatternTypes.LITERAL,
      principal: null,
      host: null,
      operation: AclOperations.READ,
      permissionType: AclPermissionTypes.DENY
    }
  ]))

await connection.close()
