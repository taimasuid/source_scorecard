#!/usr/bin/env python
"""mapper.py"""
#mapper - takes file and stores info in key,value pairs

import sys
import json


for line in sys.stdin:
    v1 = line.strip().partition('\t')[2]
    v = v1.strip().partition('\t')[2]

    if v == '':
        continue

    try:
        v = json.loads(v,'latin-1')
    except:
        continue

    #[] replaced with set() to prevent duplicates in value(sources)
    list_of_sources = set()
    for source_metadata in v.get('assertedRelationship', {}).get('sourceMetadata', []):
        list_of_sources.add(source_metadata.get('sourceAlias', ''))
    value = ','.join(list_of_sources)


    #list compression to suppress empty touchpoints
    list_of_ssns = v.get('assertedRelationship', {}).get('touchpoints', {}).get('socialSecurityNumber', [])
    #list_of_ssns = [ ssn for ssn in v.get('assertedRelationship', {}).get('touchpoints', {}).get('socialSecurityNumber', []) if ssn.get('ssn','')!='' ]
    for ssn in list_of_ssns:
        if ssn.get('socialSecurityNumber','')!='':
            key = ssn.get('socialSecurityNumber', '')
            touchpoint_type = "SSN"
            print '\t'.join([key, value,touchpoint_type])

        #testing if tab is included correctly
        #s = '\t'.join([key, value])
        #print s.partition('\t')

    list_of_phones = [ phone for phone in v.get('assertedRelationship', {}).get('touchpoints', {}).get('phone', []) if phone.get('phone','')!='' ]
    for phone in list_of_phones:
        key = phone.get('phone', '')
        touchpoint_type = "PHONE"

        print '\t'.join([key, value, touchpoint_type])


    list_of_emails = [ email for email in v.get('assertedRelationship', {}).get('touchpoints',{}).get('email',[]) if email.get('email','')!='' ]
    for email in list_of_emails:
        key = email.get('email', '')
        touchpoint_type = "EMAIL"
        print '\t'.join([key, value, touchpoint_type])

    list_of_dobs = [ dateOfBirth for dateOfBirth in v.get('assertedRelationship', {}).get('touchpoints',{}).get('dateOfBirth',[]) if dateOfBirth.get('dateOfBirth','')!='' ]
    for dateOfBirth in list_of_dobs:
        key = dateOfBirth.get('dateOfBirth', '')
        touchpoint_type = "DOB"
        print '\t'.join([key, value, touchpoint_type])


    #Combined keys to create delivery line
    list_of_addresses = (v.get('assertedRelationship', {}).get('touchpoints', {}).get('address', []))
    for address in list_of_addresses:

        pm =  address.get('primaryNumber', '')
        pre_d = address.get('preDirectional', '')
        st = address.get('street', '')
        st_suffix = address.get('streetSuffix', '')
        post_d = address.get('postDirectional', '')
        unit_des = address.get('unitDesignator',  '')
        secondary_num = address.get('secondaryNumber', '')
        zip = address.get('zipCode', '')


        address_str = ' '.join([pm, pre_d, st, st_suffix, post_d, unit_des, secondary_num, zip])
        if address_str.strip() !='':
            key = ' '.join(address_str.split())
            touchpoint_type = "ADDRESS"
            print '\t'.join([key, value, touchpoint_type])



    #Combined to create Full Name
    list_of_names = (v.get('assertedRelationship', {}).get('touchpoints', {}).get('name', []))
    for name in list_of_names:
        f_name = name.get('firstName', '')
        m_name = name.get('middleName', '')
        l_name = name.get('lastName', '')
        gen_suffix = name.get('generationalSuffix', '')

        #print full name, source
        #removes double spaces if value field is NULL
        name_str = ' '.join([f_name, m_name, l_name, gen_suffix])

        if name_str.strip() !='':
            key = ' '.join(name_str.split())
            touchpoint_type = "NAME"
            print '\t'.join([key, value, touchpoint_type])



#Edge Contribution
    for name in list_of_names:
        f_name = name.get('firstName', '')
        m_name = name.get('middleName', '')
        l_name = name.get('lastName', '')
        gen_suffix = name.get('generationalSuffix', '')

        name_str = ' '.join([f_name, m_name, l_name, gen_suffix])

        if name_str.strip() !='':
            for address in list_of_addresses:
                pm = address.get('primaryNumber', '')
                pre_d = address.get('preDirectional', '')
                st = address.get('street', '')
                st_suffix = address.get('streetSuffix', '')
                post_d = address.get('postDirectional', '')
                unit_des = address.get('unitDesignator',  '')
                secondary_num = address.get('secondaryNumber', '')
                zip = address.get('zipCode', '')

                address_str = ' '.join([pm, pre_d, st, st_suffix, post_d, unit_des, secondary_num, zip])

                key = ' | '.join([' '.join(name_str.split()),' '.join(address_str.split())])
                touchpoint_type = "NAME | ADDRESS"
                print '\t'.join([key, value, touchpoint_type])

            for phone in list_of_phones:
                key = ' | '.join([' '.join(name_str.split()), phone.get('phone', '')])
                touchpoint_type = "NAME | PHONE"
                print '\t'.join([key, value, touchpoint_type])

            for email in list_of_emails:
                key = ' | '.join([' '.join(name_str.split()), email.get('email', '')])
                touchpoint_type = "NAME | EMAIL"
                print '\t'.join([key, value, touchpoint_type])



    for address in list_of_addresses:
        pm = address.get('primaryNumber', '')
        pre_d = address.get('preDirectional', '')
        st = address.get('street', '')
        st_suffix = address.get('streetSuffix', '')
        post_d = address.get('postDirectional', '')
        unit_des = address.get('unitDesignator',  '')
        secondary_num = address.get('secondaryNumber', '')
        zip = address.get('zipCode', '')

        address_str = ' '.join([pm, pre_d, st, st_suffix, post_d, unit_des, secondary_num, zip]) 

        if address_str.strip() != '':
            for phone in list_of_phones:
                key = ' | '.join([' '.join(address_str.split()), phone.get('phone', '')])
                touchpoint_type = "ADDRESS | PHONE"
                print '\t'.join([key, value, touchpoint_type])

            for email in list_of_emails:
                key = ' | '.join([' '.join(address_str.split()), email.get('email', '')])
                touchpoint_type = "ADDRESS | EMAIL"
                print '\t'.join([key, value, touchpoint_type])



    for email in list_of_emails:
        key = email.get('email', '')

        for phone in list_of_phones: 
            key = ' | '.join([email.get('email', ''), phone.get('phone', '')])
            touchpoint_type = "EMAIL | PHONE"
            print '\t'.join([key, value, touchpoint_type])
