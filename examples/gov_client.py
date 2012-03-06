from interface.services.coi.iorg_management_service import OrgManagementServiceProcessClient
from interface.services.coi.iidentity_management_service import IdentityManagementServiceProcessClient
from interface.services.coi.ipolicy_management_service import PolicyManagementServiceProcessClient
from interface.services.sa.iinstrument_management_service import InstrumentManagementServiceProcessClient
from pyon.public import Container, RT, IonObject, CFG, log
from pyon.util.context import LocalContextMixin
from interface.services.sa.idata_product_management_service import DataProductManagementServiceProcessClient
from interface.services.sa.imarine_facility_management_service import MarineFacilityManagementServiceProcessClient
import simplejson, urllib
from ion.services.coi.service_gateway_service import GATEWAY_RESPONSE, GATEWAY_ERROR, GATEWAY_ERROR_MESSAGE, GATEWAY_ERROR_EXCEPTION, get_role_message_headers
from ion.services.coi.policy_management_service import MANAGER_ROLE, MEMBER_ROLE

class FakeProcess(LocalContextMixin):
    name = 'gov_client'
    id='gov_client'

def seed_gov(container, process=FakeProcess()):

    id_client = IdentityManagementServiceProcessClient(node=container.node, process=process)

    system_actor = id_client.find_user_identity_by_name(name=CFG.system.system_actor)
    log.info('system actor:' + system_actor._id)

    dp_client = DataProductManagementServiceProcessClient(node=container.node, process=process)

    dp_obj = IonObject(RT.DataProduct, name='DataProd1', description='some new dp')

    dp_client.create_data_product(dp_obj, headers={'ion-actor-id': system_actor._id})


    dp_obj = IonObject(RT.DataProduct,
        name='DataProd2',
        description='and of course another new dp')

    dp_client.create_data_product(dp_obj, headers={'ion-actor-id': system_actor._id})

    dp_obj = IonObject(RT.DataProduct,
        name='DataProd3',
        description='yet another new dp')

    dp_client.create_data_product(dp_obj, headers={'ion-actor-id': system_actor._id})

    log.debug('Data Products')
    dp_list = dp_client.find_data_products()
    for dp_obj in dp_list:
        log.debug( str(dp_obj))


    results = find_data_products(system_actor._id)
    log.info(results)

    ims_client = InstrumentManagementServiceProcessClient(node=container.node, process=process)

    ia_obj = IonObject(RT.InstrumentAgent, name='Instrument Agent1', description='The first Instrument Agent')

    ims_client.create_instrument_agent(ia_obj, headers={'ion-actor-id': system_actor._id})

    ia_obj = IonObject(RT.InstrumentAgent, name='Instrument Agent2', description='The second Instrument Agent')

    ims_client.create_instrument_agent(ia_obj, headers={'ion-actor-id': system_actor._id})

    log.debug( 'Instrument Agents')
    ia_list = ims_client.find_instrument_agents()
    for ia_obj in ia_list:
        log.debug( str(ia_obj))


    org_client = OrgManagementServiceProcessClient(node=container.node, process=process)
    ion_org = org_client.find_org()



    operator_role = IonObject(RT.UserRole, name='Operator',label='Instrument Operator', description='Instrument Operator')
    org_client.add_user_role(ion_org._id, operator_role, headers={'ion-actor-id': system_actor._id})

    try:
        org_client.add_user_role(ion_org._id, operator_role)
    except Exception, e:
        log.info("This should fail")
        log.info(e.message)

    certificate =  """-----BEGIN CERTIFICATE-----
MIIEMzCCAxugAwIBAgICBQAwDQYJKoZIhvcNAQEFBQAwajETMBEGCgmSJomT8ixkARkWA29yZzEX
MBUGCgmSJomT8ixkARkWB2NpbG9nb24xCzAJBgNVBAYTAlVTMRAwDgYDVQQKEwdDSUxvZ29uMRsw
GQYDVQQDExJDSUxvZ29uIEJhc2ljIENBIDEwHhcNMTAxMTE4MjIyNTA2WhcNMTAxMTE5MTAzMDA2
WjBvMRMwEQYKCZImiZPyLGQBGRMDb3JnMRcwFQYKCZImiZPyLGQBGRMHY2lsb2dvbjELMAkGA1UE
BhMCVVMxFzAVBgNVBAoTDlByb3RlY3ROZXR3b3JrMRkwFwYDVQQDExBSb2dlciBVbndpbiBBMjU0
MIIBIjANBgkqhkiG9w0BAQEFAAOCAQ8AMIIBCgKCAQEA6QhsWxhUXbIxg+1ZyEc7d+hIGvchVmtb
g0kKLmivgoVsA4U7swNDRH6svW242THta0oTf6crkRx7kOKg6jma2lcAC1sjOSddqX7/92ChoUPq
7LWt2T6GVVA10ex5WAeB/o7br/Z4U8/75uCBis+ru7xEDl09PToK20mrkcz9M4HqIv1eSoPkrs3b
2lUtQc6cjuHRDU4NknXaVMXTBHKPM40UxEDHJueFyCiZJFg3lvQuSsAl4JL5Z8pC02T8/bODBuf4
dszsqn2SC8YDw1xrujvW2Bd7Q7BwMQ/gO+dZKM1mLJFpfEsR9WrjMeg6vkD2TMWLMr0/WIkGC8u+
6M6SMQIDAQABo4HdMIHaMAwGA1UdEwEB/wQCMAAwDgYDVR0PAQH/BAQDAgSwMBMGA1UdJQQMMAoG
CCsGAQUFBwMCMBgGA1UdIAQRMA8wDQYLKwYBBAGCkTYBAgEwagYDVR0fBGMwYTAuoCygKoYoaHR0
cDovL2NybC5jaWxvZ29uLm9yZy9jaWxvZ29uLWJhc2ljLmNybDAvoC2gK4YpaHR0cDovL2NybC5k
b2Vncmlkcy5vcmcvY2lsb2dvbi1iYXNpYy5jcmwwHwYDVR0RBBgwFoEUaXRzYWdyZWVuMUB5YWhv
by5jb20wDQYJKoZIhvcNAQEFBQADggEBAEYHQPMY9Grs19MHxUzMwXp1GzCKhGpgyVKJKW86PJlr
HGruoWvx+DLNX75Oj5FC4t8bOUQVQusZGeGSEGegzzfIeOI/jWP1UtIjzvTFDq3tQMNvsgROSCx5
CkpK4nS0kbwLux+zI7BWON97UpMIzEeE05pd7SmNAETuWRsHMP+x6i7hoUp/uad4DwbzNUGIotdK
f8b270icOVgkOKRdLP/Q4r/x8skKSCRz1ZsRdR+7+B/EgksAJj7Ut3yiWoUekEMxCaTdAHPTMD/g
Mh9xL90hfMJyoGemjJswG5g3fAdTP/Lv0I6/nWeH/cLjwwpQgIEjEAVXl7KHuzX5vPD/wqQ=
-----END CERTIFICATE-----"""


    user_id, valid_until, registered = id_client.signon(certificate, True)

    log.info( "user id=" + user_id)

    try:
        org_client.enroll_member(ion_org._id,user_id)
    except Exception, e:
        log.info("This should fail:")
        log.info(e.message)


    roles = org_client.find_org_roles(ion_org._id)
    for r in roles:
        log.info('Org UserRole: ' + str(r))

    users = org_client.find_enrolled_users(ion_org._id)
    for u in users:
        log.info( str(u))
        log.info('User is enrolled in Root ION Org: %s' % str(org_client.is_enrolled(ion_org._id, user_id)))

    roles = org_client.find_roles_by_user(ion_org._id, user_id)
    for r in roles:
        log.info('User UserRole: ' + str(r))


    results = find_data_products(user_id)
    log.info(results)

    results = find_instrument_agents(user_id)
    log.info(results)

    roles = org_client.find_all_roles_by_user(system_actor._id)
    header_roles = get_role_message_headers(roles)

    org_client.grant_role(ion_org._id, user_id, 'Operator', headers={'ion-actor-id': system_actor._id, 'ion-actor-roles': header_roles })
    roles = org_client.find_roles_by_user(ion_org._id, user_id)
    for r in roles:
        log.info('User UserRole: ' +str(r))

    roles = org_client.find_roles_by_user(ion_org._id, system_actor._id)
    for r in roles:
        log.info('ION System UserRole: ' +str(r))


#    log.info("Adding Instrument Operator Role")

#    role = org_client.find_org_role_by_name(ion_org._id, 'Instrument Operator')
#    org_client.grant_role(ion_org._id, user_id, role._id)
#    roles = org_client.find_roles_by_user(ion_org._id, user_id)
#    for r in roles:
#        log.info('User UserRole: ' +str(r))


#    requests = org_client.find_requests(ion_org._id)
#    log.info("Request count: %d" % len(requests))
##    for r in requests:
#        log.info('Org Request: ' +str(r))
    '''
    org2 = IonObject(RT.Org, name='Org2', description='A second Org')
    org2_id = org_client.create_org(org2)

    requests = org_client.find_requests(org2_id)
    log.info("Org2 Request count: %d" % len(requests))
    for r in requests:
        log.info('Org2 Request: ' +str(r))

    roles = org_client.find_org_roles(org2_id)
    for r in roles:
        log.info('Org2 UserRole: ' + str(r))


    org_roles = org_client.find_all_roles_by_user(user_id)
    log.info("All Org Roles: " + str(org_roles))

    org_client.enroll_member(org2_id, user_id)


    roles = org_client.find_roles_by_user(org2_id, user_id)

    org_roles = org_client.find_all_roles_by_user(user_id)
    log.info("All Org Roles for: " + str(org_roles))

    log.info(user_id)
    '''
#    req_id = org_client.request_enroll(org2_id,user_id )

#    requests = org_client.find_requests(org2_id)
#    log.info("Org2 Request count: %d" % len(requests))
#    for r in requests:
#        log.info('Org Request: ' +str(r))

#    org_client.approve_request(org2_id, req_id)

#    requests = org_client.find_user_requests(user_id)
#    log.info("User Request count: %d" % len(requests))
#    for r in requests:
#        log.info('User Request: ' +str(r))

#    role = org_client.find_org_role_by_name(org2_id, MANAGER_ROLE)
#    req_id = org_client.request_role(org2_id,user_id, role._id)
#    requests = org_client.find_requests(org2_id)
#    log.info("Org2 Request count: %d" % len(requests))
#    for r in requests:
#        log.info('Org Request: ' +str(r))

#    marine_client = MarineFacilityManagementServiceProcessClient(node=container.node, process=process)
#    mf_obj = IonObject(RT.MarineFacility, name='Marine Facility Org', description='a new marine facility')
#    mf_id = marine_client.create_marine_facility(mf_obj)

#    roles = org_client.find_org_roles(mf_id)
#    for r in roles:
#        log.info(str(r))


#    role_obj = policy_client.find_role(MEMBER_ROLE)  # Not available anymore

#   org_client.grant_role(ion_org._id,user_id,role_obj._id)

#    members = org_client.find_enrolled_users(ion_org._id)
#    log.info("Number of Org members: %d" % len(members))

#    try:
#        org_client.remove_user_role(ion_org._id,role_obj._id)
#    except Exception, e:
#        log.info("This should fail:")
#        log.info(e.message)

#    org_client.remove_user_role(ion_org._id,role_obj._id,True)


def seed_policy(container, process=FakeProcess()):

    org_client = OrgManagementServiceProcessClient(node=container.node, process=process)
    ion_org = org_client.find_org()

    id_client = IdentityManagementServiceProcessClient(node=container.node, process=process)

    system_actor = id_client.find_user_identity_by_name(name=CFG.system.system_actor)
    log.info('system actor:' + system_actor._id)

    policy_client = PolicyManagementServiceProcessClient(node=container.node, process=process)

    policy_text = '''
    <Rule RuleId="urn:oasis:names:tc:xacml:2.0:example:ruleid:%s" Effect="Deny">
        <Description>
            %s
        </Description>

        <Target>

            <Subjects>
                <Subject>
                    <SubjectMatch MatchId="urn:oasis:names:tc:xacml:1.0:function:string-equal">
                        <AttributeValue DataType="http://www.w3.org/2001/XMLSchema#string">anonymous</AttributeValue>
                        <SubjectAttributeDesignator AttributeId="urn:oasis:names:tc:xacml:1.0:subject:subject-id" DataType="http://www.w3.org/2001/XMLSchema#string"/>
                    </SubjectMatch>
                </Subject>
            </Subjects>


            <Actions>
                <Action>
                    <ActionMatch MatchId="urn:oasis:names:tc:xacml:1.0:function:string-regexp-match">
                        <AttributeValue DataType="http://www.w3.org/2001/XMLSchema#string">create</AttributeValue>
                        <ActionAttributeDesignator AttributeId="urn:oasis:names:tc:xacml:1.0:action:action-id" DataType="http://www.w3.org/2001/XMLSchema#string"/>
                    </ActionMatch>

                    <ActionMatch MatchId="urn:oasis:names:tc:xacml:1.0:function:string-regexp-match">
                        <AttributeValue DataType="http://www.w3.org/2001/XMLSchema#string">update</AttributeValue>
                        <ActionAttributeDesignator AttributeId="urn:oasis:names:tc:xacml:1.0:action:action-id" DataType="http://www.w3.org/2001/XMLSchema#string"/>
                    </ActionMatch>

                    <ActionMatch MatchId="urn:oasis:names:tc:xacml:1.0:function:string-regexp-match">
                        <AttributeValue DataType="http://www.w3.org/2001/XMLSchema#string">delete</AttributeValue>
                        <ActionAttributeDesignator AttributeId="urn:oasis:names:tc:xacml:1.0:action:action-id" DataType="http://www.w3.org/2001/XMLSchema#string"/>
                    </ActionMatch>

                    <ActionMatch MatchId="urn:oasis:names:tc:xacml:1.0:function:string-regexp-match">
                        <AttributeValue DataType="http://www.w3.org/2001/XMLSchema#string">activate</AttributeValue>
                        <ActionAttributeDesignator AttributeId="urn:oasis:names:tc:xacml:1.0:action:action-id" DataType="http://www.w3.org/2001/XMLSchema#string"/>
                    </ActionMatch>

                    <ActionMatch MatchId="urn:oasis:names:tc:xacml:1.0:function:string-regexp-match">
                        <AttributeValue DataType="http://www.w3.org/2001/XMLSchema#string">deactivate</AttributeValue>
                        <ActionAttributeDesignator AttributeId="urn:oasis:names:tc:xacml:1.0:action:action-id" DataType="http://www.w3.org/2001/XMLSchema#string"/>
                    </ActionMatch>

                    <ActionMatch MatchId="urn:oasis:names:tc:xacml:1.0:function:string-regexp-match">
                        <AttributeValue DataType="http://www.w3.org/2001/XMLSchema#string">add</AttributeValue>
                        <ActionAttributeDesignator AttributeId="urn:oasis:names:tc:xacml:1.0:action:action-id" DataType="http://www.w3.org/2001/XMLSchema#string"/>
                    </ActionMatch>

                    <ActionMatch MatchId="urn:oasis:names:tc:xacml:1.0:function:string-regexp-match">
                        <AttributeValue DataType="http://www.w3.org/2001/XMLSchema#string">remove</AttributeValue>
                        <ActionAttributeDesignator AttributeId="urn:oasis:names:tc:xacml:1.0:action:action-id" DataType="http://www.w3.org/2001/XMLSchema#string"/>
                    </ActionMatch>

                    <ActionMatch MatchId="urn:oasis:names:tc:xacml:1.0:function:string-regexp-match">
                        <AttributeValue DataType="http://www.w3.org/2001/XMLSchema#string">assign</AttributeValue>
                        <ActionAttributeDesignator AttributeId="urn:oasis:names:tc:xacml:1.0:action:action-id" DataType="http://www.w3.org/2001/XMLSchema#string"/>
                    </ActionMatch>

                    <ActionMatch MatchId="urn:oasis:names:tc:xacml:1.0:function:string-regexp-match">
                        <AttributeValue DataType="http://www.w3.org/2001/XMLSchema#string">unassign</AttributeValue>
                        <ActionAttributeDesignator AttributeId="urn:oasis:names:tc:xacml:1.0:action:action-id" DataType="http://www.w3.org/2001/XMLSchema#string"/>
                    </ActionMatch>

                    <ActionMatch MatchId="urn:oasis:names:tc:xacml:1.0:function:string-regexp-match">
                        <AttributeValue DataType="http://www.w3.org/2001/XMLSchema#string">register</AttributeValue>
                        <ActionAttributeDesignator AttributeId="urn:oasis:names:tc:xacml:1.0:action:action-id" DataType="http://www.w3.org/2001/XMLSchema#string"/>
                    </ActionMatch>

                    <ActionMatch MatchId="urn:oasis:names:tc:xacml:1.0:function:string-regexp-match">
                        <AttributeValue DataType="http://www.w3.org/2001/XMLSchema#string">unregister</AttributeValue>
                        <ActionAttributeDesignator AttributeId="urn:oasis:names:tc:xacml:1.0:action:action-id" DataType="http://www.w3.org/2001/XMLSchema#string"/>
                    </ActionMatch>

                    <ActionMatch MatchId="urn:oasis:names:tc:xacml:1.0:function:string-regexp-match">
                        <AttributeValue DataType="http://www.w3.org/2001/XMLSchema#string">execute</AttributeValue>
                        <ActionAttributeDesignator AttributeId="urn:oasis:names:tc:xacml:1.0:action:action-id" DataType="http://www.w3.org/2001/XMLSchema#string"/>
                    </ActionMatch>

                    <ActionMatch MatchId="urn:oasis:names:tc:xacml:1.0:function:string-regexp-match">
                        <AttributeValue DataType="http://www.w3.org/2001/XMLSchema#string">set</AttributeValue>
                        <ActionAttributeDesignator AttributeId="urn:oasis:names:tc:xacml:1.0:action:action-id" DataType="http://www.w3.org/2001/XMLSchema#string"/>
                    </ActionMatch>

                </Action>
            </Actions>

        </Target>


    </Rule>
    '''


    policy_obj = IonObject(RT.Policy, name='Anonymous_No_Create_Update', definition_type="global", rule=policy_text,
        description='A global rule which specifies that an anonymous user can access most services except create/update operations')

    policy_id = policy_client.create_policy(policy_obj, headers={'ion-actor-id': system_actor._id})
    policy_client.add_resource_policy(ion_org._id, policy_id, headers={'ion-actor-id': system_actor._id})


    policy_text = '''
        <Rule RuleId="urn:oasis:names:tc:xacml:2.0:example:ruleid:%s" Effect="Permit">
        <Description>
            %s
        </Description>

        <Target>

            <Subjects>
                <Subject>
                    <SubjectMatch MatchId="urn:oasis:names:tc:xacml:1.0:function:string-equal">
                        <AttributeValue DataType="http://www.w3.org/2001/XMLSchema#string">anonymous</AttributeValue>
                        <SubjectAttributeDesignator AttributeId="urn:oasis:names:tc:xacml:1.0:subject:subject-id" DataType="http://www.w3.org/2001/XMLSchema#string"/>
                    </SubjectMatch>
                </Subject>
            </Subjects>

            <Resources>
                <Resource>
                    <ResourceMatch MatchId="urn:oasis:names:tc:xacml:1.0:function:string-equal">
                        <AttributeValue DataType="http://www.w3.org/2001/XMLSchema#string">datastore</AttributeValue>
                        <ResourceAttributeDesignator AttributeId="urn:oasis:names:tc:xacml:1.0:resource:resource-id" DataType="http://www.w3.org/2001/XMLSchema#string"/>
                    </ResourceMatch>
                </Resource>
            </Resources>

            <Actions>
                <Action>
                    <ActionMatch MatchId="urn:oasis:names:tc:xacml:1.0:function:string-equal">
                        <AttributeValue DataType="http://www.w3.org/2001/XMLSchema#string">create_doc</AttributeValue>
                        <ActionAttributeDesignator AttributeId="urn:oasis:names:tc:xacml:1.0:action:action-id" DataType="http://www.w3.org/2001/XMLSchema#string"/>
                    </ActionMatch>
                </Action>
            </Actions>

        </Target>

        <Condition>

            <Apply FunctionId="urn:oasis:names:tc:xacml:1.0:function:string-equal">
                <Apply FunctionId="urn:oasis:names:tc:xacml:1.0:function:string-one-and-only">
                    <SubjectAttributeDesignator
                            AttributeId="urn:oasis:names:tc:xacml:1.0:subject:subject-id-sender"
                            DataType="http://www.w3.org/2001/XMLSchema#string"/>
                </Apply>
                <AttributeValue DataType="http://www.w3.org/2001/XMLSchema#string">bootstrap</AttributeValue>
            </Apply>

        </Condition>

    </Rule>
    '''

    policy_obj = IonObject(RT.Policy, name='DataStore_Anonymous_Bootstrap', definition_type="service", rule=policy_text,
        description='An anonymous user can only access datastore.create_doc during bootstrap')

    policy_id = policy_client.create_policy(policy_obj)
    policy_client.add_service_policy('datastore', policy_id)



    policy_text = '''
    <Rule RuleId="urn:oasis:names:tc:xacml:2.0:example:ruleid:%s" Effect="Permit">
        <Description>
            %s

        </Description>

        <Target>

            <Subjects>
                <Subject>
                    <SubjectMatch MatchId="urn:oasis:names:tc:xacml:1.0:function:string-equal">
                        <AttributeValue DataType="http://www.w3.org/2001/XMLSchema#string">anonymous</AttributeValue>
                        <SubjectAttributeDesignator AttributeId="urn:oasis:names:tc:xacml:1.0:subject:subject-id" DataType="http://www.w3.org/2001/XMLSchema#string"/>
                    </SubjectMatch>
                </Subject>
            </Subjects>

            <Resources>
                <Resource>
                    <ResourceMatch MatchId="urn:oasis:names:tc:xacml:1.0:function:string-equal">
                        <AttributeValue DataType="http://www.w3.org/2001/XMLSchema#string">identity_management</AttributeValue>
                        <ResourceAttributeDesignator AttributeId="urn:oasis:names:tc:xacml:1.0:resource:resource-id" DataType="http://www.w3.org/2001/XMLSchema#string"/>
                    </ResourceMatch>
                </Resource>
            </Resources>

            <Actions>
                <Action>
                    <ActionMatch MatchId="urn:oasis:names:tc:xacml:1.0:function:string-equal">
                        <AttributeValue DataType="http://www.w3.org/2001/XMLSchema#string">create_user_identity</AttributeValue>
                        <ActionAttributeDesignator AttributeId="urn:oasis:names:tc:xacml:1.0:action:action-id" DataType="http://www.w3.org/2001/XMLSchema#string"/>
                    </ActionMatch>
                </Action>
            </Actions>

        </Target>

        <Condition>

            <Apply FunctionId="urn:oasis:names:tc:xacml:1.0:function:string-equal">
                <Apply FunctionId="urn:oasis:names:tc:xacml:1.0:function:string-one-and-only">
                    <SubjectAttributeDesignator
                            AttributeId="urn:oasis:names:tc:xacml:1.0:subject:subject-id-sender"
                            DataType="http://www.w3.org/2001/XMLSchema#string"/>
                </Apply>
                <AttributeValue DataType="http://www.w3.org/2001/XMLSchema#string">bootstrap</AttributeValue>
            </Apply>

        </Condition>
    </Rule>
    '''

    policy_obj = IonObject(RT.Policy, name='Identity_Management_Anonymous_Bootstrap', definition_type="service", rule=policy_text,
        description='An anonymous user can onbly access identity_management.create_user_identity during bootstrap')

    policy_id = policy_client.create_policy(policy_obj)
    policy_client.add_service_policy('identity_management', policy_id)


    policy_text = '''
       <Rule RuleId="urn:oasis:names:tc:xacml:2.0:example:ruleid:%s" Effect="Permit">
        <Description>
            %s

        </Description>

        <Target>

            <Subjects>
                <Subject>
                    <SubjectMatch MatchId="urn:oasis:names:tc:xacml:1.0:function:string-equal">
                        <AttributeValue DataType="http://www.w3.org/2001/XMLSchema#string">anonymous</AttributeValue>
                        <SubjectAttributeDesignator AttributeId="urn:oasis:names:tc:xacml:1.0:subject:subject-id" DataType="http://www.w3.org/2001/XMLSchema#string"/>
                    </SubjectMatch>
                </Subject>
            </Subjects>

            <Resources>
                <Resource>
                    <ResourceMatch MatchId="urn:oasis:names:tc:xacml:1.0:function:string-equal">
                        <AttributeValue DataType="http://www.w3.org/2001/XMLSchema#string">resource_registry</AttributeValue>
                        <ResourceAttributeDesignator AttributeId="urn:oasis:names:tc:xacml:1.0:resource:resource-id" DataType="http://www.w3.org/2001/XMLSchema#string"/>
                    </ResourceMatch>
                </Resource>
            </Resources>


            <Actions>
                <Action>
                    <ActionMatch MatchId="urn:oasis:names:tc:xacml:1.0:function:string-equal">
                        <ActionAttributeDesignator AttributeId="urn:oasis:names:tc:xacml:1.0:action:action-id" DataType="http://www.w3.org/2001/XMLSchema#string"/>
                        <AttributeValue DataType="http://www.w3.org/2001/XMLSchema#string">create</AttributeValue>
                    </ActionMatch>
                </Action>
                <Action>
                    <ActionMatch MatchId="urn:oasis:names:tc:xacml:1.0:function:string-equal">
                        <ActionAttributeDesignator AttributeId="urn:oasis:names:tc:xacml:1.0:action:action-id" DataType="http://www.w3.org/2001/XMLSchema#string"/>
                        <AttributeValue DataType="http://www.w3.org/2001/XMLSchema#string">create_association</AttributeValue>
                    </ActionMatch>
                </Action>
            </Actions>

        </Target>
        <Condition>

            <Apply FunctionId="urn:oasis:names:tc:xacml:1.0:function:string-equal">
                <Apply FunctionId="urn:oasis:names:tc:xacml:1.0:function:string-one-and-only">
                    <SubjectAttributeDesignator
                            AttributeId="urn:oasis:names:tc:xacml:1.0:subject:subject-id-sender"
                            DataType="http://www.w3.org/2001/XMLSchema#string"/>
                </Apply>
                <AttributeValue DataType="http://www.w3.org/2001/XMLSchema#string">identity_management</AttributeValue>
            </Apply>

        </Condition>

    </Rule>
    '''


    policy_obj = IonObject(RT.Policy, name='Resource_Registry_Anonymous_Bootstrap', definition_type="service", rule=policy_text,
        description='An anonymous user can only access resource_registry.create and resource_registry.create_association from the identity_management service during bootstrap')

    policy_id = policy_client.create_policy(policy_obj)
    policy_client.add_service_policy('resource_registry', policy_id)


    policy_text = '''
        <Rule RuleId="urn:oasis:names:tc:xacml:2.0:example:ruleid:%s" Effect="Permit">
        <Description>
            %s
        </Description>

        <Target>

            <Subjects>
                <Subject>
                    <SubjectMatch MatchId="urn:oasis:names:tc:xacml:1.0:function:string-equal">
                        <AttributeValue DataType="http://www.w3.org/2001/XMLSchema#string">True</AttributeValue>
                        <SubjectAttributeDesignator AttributeId="urn:oasis:names:tc:xacml:1.0:subject:subject-role-Manager" DataType="http://www.w3.org/2001/XMLSchema#string"/>
                    </SubjectMatch>
                </Subject>
            </Subjects>

            <Resources>
                <Resource>
                    <ResourceMatch MatchId="urn:oasis:names:tc:xacml:1.0:function:string-equal">
                        <AttributeValue DataType="http://www.w3.org/2001/XMLSchema#string">org_management</AttributeValue>
                        <ResourceAttributeDesignator AttributeId="urn:oasis:names:tc:xacml:1.0:resource:resource-id" DataType="http://www.w3.org/2001/XMLSchema#string"/>
                    </ResourceMatch>
                </Resource>
            </Resources>

            <Actions>
                <Action>
                    <ActionMatch MatchId="urn:oasis:names:tc:xacml:1.0:function:string-regexp-match">
                        <AttributeValue DataType="http://www.w3.org/2001/XMLSchema#string">grant_role</AttributeValue>
                        <ActionAttributeDesignator AttributeId="urn:oasis:names:tc:xacml:1.0:action:action-id" DataType="http://www.w3.org/2001/XMLSchema#string"/>
                    </ActionMatch>
                </Action>
                <Action>
                    <ActionMatch MatchId="urn:oasis:names:tc:xacml:1.0:function:string-regexp-match">
                        <AttributeValue DataType="http://www.w3.org/2001/XMLSchema#string">revoke_role</AttributeValue>
                        <ActionAttributeDesignator AttributeId="urn:oasis:names:tc:xacml:1.0:action:action-id" DataType="http://www.w3.org/2001/XMLSchema#string"/>
                    </ActionMatch>
                </Action>
            </Actions>

        </Target>


    </Rule> '''

    policy_obj = IonObject(RT.Policy, name='Org_Management_Manager_Role_Permitted', definition_type="service", rule=policy_text,
        description='Specific operations in the Org Management are only allowed for users that are assigned the User Role of Manager')

    policy_id = policy_client.create_policy(policy_obj)
    policy_client.add_service_policy('org_management', policy_id)

    policy_text = '''
        <Rule RuleId="urn:oasis:names:tc:xacml:2.0:example:ruleid:%s" Effect="Deny">
        <Description>
            %s
        </Description>

        <Target>

            <Subjects>
                <Subject>
                    <SubjectMatch MatchId="urn:oasis:names:tc:xacml:1.0:function:string-equal">
                        <AttributeValue DataType="http://www.w3.org/2001/XMLSchema#string">True</AttributeValue>
                        <SubjectAttributeDesignator AttributeId="urn:oasis:names:tc:xacml:1.0:subject:subject-role-Member" DataType="http://www.w3.org/2001/XMLSchema#string"/>
                    </SubjectMatch>
                </Subject>
                <Subject>
                    <SubjectMatch MatchId="urn:oasis:names:tc:xacml:1.0:function:string-equal">
                        <AttributeValue DataType="http://www.w3.org/2001/XMLSchema#string">anonymous</AttributeValue>
                        <SubjectAttributeDesignator AttributeId="urn:oasis:names:tc:xacml:1.0:subject:subject-id" DataType="http://www.w3.org/2001/XMLSchema#string"/>
                    </SubjectMatch>
                </Subject>
            </Subjects>

            <Resources>
                <Resource>
                    <ResourceMatch MatchId="urn:oasis:names:tc:xacml:1.0:function:string-equal">
                        <AttributeValue DataType="http://www.w3.org/2001/XMLSchema#string">org_management</AttributeValue>
                        <ResourceAttributeDesignator AttributeId="urn:oasis:names:tc:xacml:1.0:resource:resource-id" DataType="http://www.w3.org/2001/XMLSchema#string"/>
                    </ResourceMatch>
                </Resource>
            </Resources>

            <Actions>
                <Action>
                    <ActionMatch MatchId="urn:oasis:names:tc:xacml:1.0:function:string-regexp-match">
                        <AttributeValue DataType="http://www.w3.org/2001/XMLSchema#string">grant_role</AttributeValue>
                        <ActionAttributeDesignator AttributeId="urn:oasis:names:tc:xacml:1.0:action:action-id" DataType="http://www.w3.org/2001/XMLSchema#string"/>
                    </ActionMatch>
                </Action>
                <Action>
                    <ActionMatch MatchId="urn:oasis:names:tc:xacml:1.0:function:string-regexp-match">
                        <AttributeValue DataType="http://www.w3.org/2001/XMLSchema#string">revoke_role</AttributeValue>
                        <ActionAttributeDesignator AttributeId="urn:oasis:names:tc:xacml:1.0:action:action-id" DataType="http://www.w3.org/2001/XMLSchema#string"/>
                    </ActionMatch>
                </Action>
            </Actions>

        </Target>


    </Rule>
    '''
    policy_obj = IonObject(RT.Policy, name='Org_Management_Member_Role_Denied', definition_type="service", rule=policy_text,
        description='Specific operations in the Org Management are not allowed for users that are only assigned the User Role of Member')

    policy_id = policy_client.create_policy(policy_obj)
    policy_client.add_service_policy('org_management', policy_id)

    ion_org_policy = policy_client.get_active_resource_policy_rules(ion_org._id)
    print ion_org_policy


    org_mgmt_policy = policy_client.get_active_service_policy_rules(ion_org._id,'org_management' )
    print org_mgmt_policy


def gateway_request(uri, payload):

    server_hostname = 'localhost'
    server_port = 5000
    web_server_cfg = None
    try:
        web_server_cfg = CFG['container']['service_gateway']['web_server']
    except Exception, e:
        web_server_cfg = None

    if web_server_cfg is not None:
        if 'hostname' in web_server_cfg:
            server_hostname = web_server_cfg['hostname']
        if 'port' in web_server_cfg:
            server_port = web_server_cfg['port']



    SEARCH_BASE = 'http://' + server_hostname + ':' + str(server_port) + '/ion-service/' + uri


    args = {}
    args.update({
        #'format': "unix",
        #'output': 'json'
    })
    url = SEARCH_BASE + '?' + urllib.urlencode(args)
    log.debug(url)
    log.debug(payload)

    result = simplejson.load(urllib.urlopen(url, 'payload=' + str(payload ) ))
    if not result.has_key('data'):
        log.error('Not a correct JSON response: %s' & result)

    return result

def find_data_products(requester=None):

    """
    data_product_find_request = {  "serviceRequest": {
        "serviceName": "resource_registry",
        "serviceOp": "find_resources",
        "requester": requester,
        "expiry": 0,
        "params": {
            "restype": 'DataProduct',
            "id_only": False
        }
    }
    }

    response = gateway_request('resource_registry/find_resources',  simplejson.dumps(data_product_find_request) )
    """

    data_product_find_request = {  "serviceRequest": {
        "serviceName": "data_product_management",
        "serviceOp": "find_data_products",
        "expiry": 0,
        "params": {
        }
    }
    }

    if requester is not None:
        data_product_find_request["serviceRequest"]["requester"] = requester

    response = gateway_request('data_product_management/find_data_products',  simplejson.dumps(data_product_find_request) )


    if response['data'].has_key(GATEWAY_ERROR):
        log.error(response['data'][GATEWAY_ERROR][GATEWAY_ERROR_MESSAGE])
        return response['data'][GATEWAY_ERROR][GATEWAY_ERROR_MESSAGE]

    response_data = response['data'][GATEWAY_RESPONSE]

    log.info('Number of DataProduct objects: %s' % (str(len(response_data))))
    for res in response_data:
        log.debug(res)

    return response_data

def find_instrument_agents( requester=None):


    instrument_agent_find_request = {  "serviceRequest": {
        "serviceName": "instrument_management",
        "serviceOp": "find_instrument_agents",
        "expiry": 0,
        "params": {
        }
    }
    }

    if requester is not None:
        instrument_agent_find_request["serviceRequest"]["requester"] = requester

    response = gateway_request('instrument_management/find_instrument_agents',  simplejson.dumps(instrument_agent_find_request) )


    if response['data'].has_key(GATEWAY_ERROR):
        log.error(response['data'][GATEWAY_ERROR][GATEWAY_ERROR_MESSAGE])
        return response['data'][GATEWAY_ERROR][GATEWAY_ERROR_MESSAGE]

    response_data = response['data'][GATEWAY_RESPONSE]

    log.info('Number of Instrument Agent objects: %s' % (str(len(response_data))))
    for res in response_data:
        log.debug(res)

    return response_data

def find_org_roles(requester=''):

    find_org_request = {  "serviceRequest": {
        "serviceName": "org_management",
        "serviceOp": "find_org",
        "expiry": 0,
        "params": {
        }
    }
    }

    if requester is not None:
        find_org_request["serviceRequest"]["requester"] = requester

    response = gateway_request('org_management/find_org',  simplejson.dumps(find_org_request) )


    if response['data'].has_key(GATEWAY_ERROR):
        log.error(response['data'][GATEWAY_ERROR][GATEWAY_ERROR_MESSAGE])
        return response['data'][GATEWAY_ERROR][GATEWAY_ERROR_MESSAGE]

    response_data = response['data'][GATEWAY_RESPONSE]
    ion_org_id = response_data['_id']


    find_org_roles_request = {  "serviceRequest": {
        "serviceName": "org_management",
        "serviceOp": "find_org_roles",
        "expiry": 0,
        "params": {
            "org_id": ion_org_id
        }
    }
    }

    if requester is not None:
        find_org_roles_request["serviceRequest"]["requester"] = requester

    response = gateway_request('org_management/find_org_roles',  simplejson.dumps(find_org_roles_request) )


    if response['data'].has_key(GATEWAY_ERROR):
        log.error(response['data'][GATEWAY_ERROR][GATEWAY_ERROR_MESSAGE])
        return response['data'][GATEWAY_ERROR][GATEWAY_ERROR_MESSAGE]

    response_data = response['data'][GATEWAY_RESPONSE]

    log.info('Number of UserRole objects in the ION Org: %s' % (str(len(response_data))))
    for res in response_data:
        log.debug(res)

    return response_data



if __name__ == '__main__':

    container = Container()
    container.start() # :(
    seed_gov(container)
    container.stop()