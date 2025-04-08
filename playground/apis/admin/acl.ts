import { createAclsV3 } from '../../../src/apis/admin/create-acls.ts'
import { deleteAclsV3 } from '../../../src/apis/admin/delete-acls.ts'
import { describeAclsV3 } from '../../../src/apis/admin/describe-acls.ts'
import {
  AclOperations,
  AclPermissionTypes,
  ResourcePatternTypes,
  ResourceTypes
} from '../../../src/apis/enumerations.ts'
import { Connection } from '../../../src/network/connection.ts'
import { performAPICallWithRetry } from '../../utils.ts'

const connection = new Connection('123')
await connection.connect('localhost', 9092)

await performAPICallWithRetry('CreateAcls', () =>
  createAclsV3.async(connection, [
    {
      resourceType: ResourceTypes.TOPIC,
      resourceName: 'temp',
      resourcePatternType: ResourcePatternTypes.LITERAL,
      principal: 'abc:cde',
      host: '*',
      operation: AclOperations.READ,
      permissionType: AclPermissionTypes.DENY
    }
  ])
)

await performAPICallWithRetry('DescribeAcls', () =>
  describeAclsV3.async(
    connection,
    ResourceTypes.TOPIC,
    'temp',
    ResourcePatternTypes.LITERAL,
    null,
    null,
    AclOperations.READ,
    AclPermissionTypes.DENY
  )
)

await performAPICallWithRetry('DescribeAcls', () =>
  describeAclsV3.async(
    connection,
    ResourceTypes.TOPIC,
    'temp',
    ResourcePatternTypes.LITERAL,
    null,
    null,
    AclOperations.READ,
    AclPermissionTypes.ALLOW
  )
)

await performAPICallWithRetry('DeleteAcls', () =>
  deleteAclsV3.async(connection, [
    {
      resourceTypeFilter: ResourceTypes.TOPIC,
      resourceNameFilter: 'temp',
      patternTypeFilter: ResourcePatternTypes.LITERAL,
      principalFilter: null,
      hostFilter: null,
      operation: AclOperations.READ,
      permissionType: AclPermissionTypes.DENY
    }
  ])
)

await connection.close()
