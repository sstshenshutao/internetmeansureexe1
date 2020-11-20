from unittest import TestCase

from dao import Dao


def infer_create_table_sql(table_name, json_data):
    table_schema = "CREATE TABLE " + table_name + "\n("
    is_first = True
    for k in json_data.keys():
        if is_first:
            table_schema += k
            is_first = False
        else:
            table_schema += (", " + k)
    table_schema += ")"
    return table_schema


class Test(TestCase):
    alexa_a_struct = {'query_type': 'A', 'query_name': 'advaiya.com.', 'response_type': 'A',
                      'response_name': 'advaiya.com.',
                      'response_ttl': 1800, 'timestamp': 1577839842000, 'rtt': 0.004500448703765869,
                      'worker_id': 2020174261,
                      'status_code': 0, 'ad_flag': 0, 'ip4_address': '162.222.226.77', 'ip6_address': None,
                      'country': 'US',
                      'as': '394695', 'as_full': '[394695]', 'ip_prefix': '162.222.226.0/24', 'cname_name': None,
                      'dname_name': None, 'mx_address': None, 'mx_preference': None, 'mxset_hash_algorithm': None,
                      'mxset_hash': None, 'ns_address': None, 'nsset_hash_algorithm': None, 'nsset_hash': None,
                      'txt_text': None,
                      'txt_hash_algorithm': None, 'txt_hash': None, 'ds_key_tag': None, 'ds_algorithm': None,
                      'ds_digest_type': None, 'ds_digest': None, 'dnskey_flags': None, 'dnskey_protocol': None,
                      'dnskey_algorithm': None, 'dnskey_pk_rsa_n': None, 'dnskey_pk_rsa_e': None,
                      'dnskey_pk_rsa_bitsize': None,
                      'dnskey_pk_eccgost_x': None, 'dnskey_pk_eccgost_y': None, 'dnskey_pk_dsa_t': None,
                      'dnskey_pk_dsa_q': None,
                      'dnskey_pk_dsa_p': None, 'dnskey_pk_dsa_g': None, 'dnskey_pk_dsa_y': None,
                      'dnskey_pk_eddsa_a': None,
                      'dnskey_pk_wire': None, 'nsec_next_domain_name': None, 'nsec_owner_rrset_types': None,
                      'nsec3_hash_algorithm': None, 'nsec3_flags': None, 'nsec3_iterations': None, 'nsec3_salt': None,
                      'nsec3_next_domain_name_hash': None, 'nsec3_owner_rrset_types': None,
                      'nsec3param_hash_algorithm': None,
                      'nsec3param_flags': None, 'nsec3param_iterations': None, 'nsec3param_salt': None,
                      'spf_text': None,
                      'spf_hash_algorithm': None, 'spf_hash': None, 'soa_mname': None, 'soa_rname': None,
                      'soa_serial': None,
                      'soa_refresh': None, 'soa_retry': None, 'soa_expire': None, 'soa_minimum': None,
                      'rrsig_type_covered': None,
                      'rrsig_algorithm': None, 'rrsig_labels': None, 'rrsig_original_ttl': None,
                      'rrsig_signature_inception': None, 'rrsig_signature_expiration': None, 'rrsig_key_tag': None,
                      'rrsig_signer_name': None, 'rrsig_signature': None, 'cds_key_tag': None, 'cds_algorithm': None,
                      'cds_digest_type': None, 'cds_digest': None, 'cdnskey_flags': None, 'cdnskey_protocol': None,
                      'cdnskey_algorithm': None, 'cdnskey_pk_rsa_n': None, 'cdnskey_pk_rsa_e': None,
                      'cdnskey_pk_rsa_bitsize': None, 'cdnskey_pk_eccgost_x': None, 'cdnskey_pk_eccgost_y': None,
                      'cdnskey_pk_dsa_t': None, 'cdnskey_pk_dsa_q': None, 'cdnskey_pk_dsa_p': None,
                      'cdnskey_pk_dsa_g': None,
                      'cdnskey_pk_dsa_y': None, 'cdnskey_pk_eddsa_a': None, 'cdnskey_pk_wire': None, 'caa_flags': None,
                      'caa_tag': None, 'caa_value': None, 'tlsa_usage': None, 'tlsa_selector': None,
                      'tlsa_matchtype': None,
                      'tlsa_certdata': None, 'ptr_name': None}
    alexa_cname_struct = {'query_type': 'SOA', 'query_name': 'acmss.myshopify.com.', 'response_type': 'CNAME',
                          'response_name': 'acmss.myshopify.com.', 'response_ttl': 3600, 'timestamp': 1577839834000,
                          'rtt': 0.04061019420623779, 'worker_id': 2020174261, 'status_code': 0, 'ad_flag': 0,
                          'ip4_address': None, 'ip6_address': None, 'country': None, 'as': None, 'as_full': None,
                          'ip_prefix': None, 'cname_name': 'shops.myshopify.com.', 'dname_name': None,
                          'mx_address': None,
                          'mx_preference': None, 'mxset_hash_algorithm': None, 'mxset_hash': None, 'ns_address': None,
                          'nsset_hash_algorithm': None, 'nsset_hash': None, 'txt_text': None,
                          'txt_hash_algorithm': None,
                          'txt_hash': None, 'ds_key_tag': None, 'ds_algorithm': None, 'ds_digest_type': None,
                          'ds_digest': None, 'dnskey_flags': None, 'dnskey_protocol': None, 'dnskey_algorithm': None,
                          'dnskey_pk_rsa_n': None, 'dnskey_pk_rsa_e': None, 'dnskey_pk_rsa_bitsize': None,
                          'dnskey_pk_eccgost_x': None, 'dnskey_pk_eccgost_y': None, 'dnskey_pk_dsa_t': None,
                          'dnskey_pk_dsa_q': None, 'dnskey_pk_dsa_p': None, 'dnskey_pk_dsa_g': None,
                          'dnskey_pk_dsa_y': None, 'dnskey_pk_eddsa_a': None, 'dnskey_pk_wire': None,
                          'nsec_next_domain_name': None, 'nsec_owner_rrset_types': None, 'nsec3_hash_algorithm': None,
                          'nsec3_flags': None, 'nsec3_iterations': None, 'nsec3_salt': None,
                          'nsec3_next_domain_name_hash': None, 'nsec3_owner_rrset_types': None,
                          'nsec3param_hash_algorithm': None, 'nsec3param_flags': None, 'nsec3param_iterations': None,
                          'nsec3param_salt': None, 'spf_text': None, 'spf_hash_algorithm': None, 'spf_hash': None,
                          'soa_mname': None, 'soa_rname': None, 'soa_serial': None, 'soa_refresh': None,
                          'soa_retry': None, 'soa_expire': None, 'soa_minimum': None, 'rrsig_type_covered': None,
                          'rrsig_algorithm': None, 'rrsig_labels': None, 'rrsig_original_ttl': None,
                          'rrsig_signature_inception': None, 'rrsig_signature_expiration': None, 'rrsig_key_tag': None,
                          'rrsig_signer_name': None, 'rrsig_signature': None, 'cds_key_tag': None,
                          'cds_algorithm': None,
                          'cds_digest_type': None, 'cds_digest': None, 'cdnskey_flags': None, 'cdnskey_protocol': None,
                          'cdnskey_algorithm': None, 'cdnskey_pk_rsa_n': None, 'cdnskey_pk_rsa_e': None,
                          'cdnskey_pk_rsa_bitsize': None, 'cdnskey_pk_eccgost_x': None, 'cdnskey_pk_eccgost_y': None,
                          'cdnskey_pk_dsa_t': None, 'cdnskey_pk_dsa_q': None, 'cdnskey_pk_dsa_p': None,
                          'cdnskey_pk_dsa_g': None, 'cdnskey_pk_dsa_y': None, 'cdnskey_pk_eddsa_a': None,
                          'cdnskey_pk_wire': None, 'caa_flags': None, 'caa_tag': None, 'caa_value': None,
                          'tlsa_usage': None, 'tlsa_selector': None, 'tlsa_matchtype': None, 'tlsa_certdata': None,
                          'ptr_name': None}
    alexa_aaaa_struct = {'query_type': 'AAAA', 'query_name': 'www.acodeza.com.', 'response_type': 'AAAA',
                         'response_name': 'ghs.google.com.', 'response_ttl': 44, 'timestamp': 1577839834000,
                         'rtt': 0.0006458163261413574, 'worker_id': 2020174261, 'status_code': 0, 'ad_flag': 0,
                         'ip4_address': None, 'ip6_address': '2a00:1450:400e:80c::2013', 'country': '--', 'as': '15169',
                         'as_full': '[15169]', 'ip_prefix': '2a00:1450:400e::/48', 'cname_name': None,
                         'dname_name': None,
                         'mx_address': None, 'mx_preference': None, 'mxset_hash_algorithm': None, 'mxset_hash': None,
                         'ns_address': None, 'nsset_hash_algorithm': None, 'nsset_hash': None, 'txt_text': None,
                         'txt_hash_algorithm': None, 'txt_hash': None, 'ds_key_tag': None, 'ds_algorithm': None,
                         'ds_digest_type': None, 'ds_digest': None, 'dnskey_flags': None, 'dnskey_protocol': None,
                         'dnskey_algorithm': None, 'dnskey_pk_rsa_n': None, 'dnskey_pk_rsa_e': None,
                         'dnskey_pk_rsa_bitsize': None, 'dnskey_pk_eccgost_x': None, 'dnskey_pk_eccgost_y': None,
                         'dnskey_pk_dsa_t': None, 'dnskey_pk_dsa_q': None, 'dnskey_pk_dsa_p': None,
                         'dnskey_pk_dsa_g': None, 'dnskey_pk_dsa_y': None, 'dnskey_pk_eddsa_a': None,
                         'dnskey_pk_wire': None, 'nsec_next_domain_name': None, 'nsec_owner_rrset_types': None,
                         'nsec3_hash_algorithm': None, 'nsec3_flags': None, 'nsec3_iterations': None,
                         'nsec3_salt': None,
                         'nsec3_next_domain_name_hash': None, 'nsec3_owner_rrset_types': None,
                         'nsec3param_hash_algorithm': None, 'nsec3param_flags': None, 'nsec3param_iterations': None,
                         'nsec3param_salt': None, 'spf_text': None, 'spf_hash_algorithm': None, 'spf_hash': None,
                         'soa_mname': None, 'soa_rname': None, 'soa_serial': None, 'soa_refresh': None,
                         'soa_retry': None,
                         'soa_expire': None, 'soa_minimum': None, 'rrsig_type_covered': None, 'rrsig_algorithm': None,
                         'rrsig_labels': None, 'rrsig_original_ttl': None, 'rrsig_signature_inception': None,
                         'rrsig_signature_expiration': None, 'rrsig_key_tag': None, 'rrsig_signer_name': None,
                         'rrsig_signature': None, 'cds_key_tag': None, 'cds_algorithm': None, 'cds_digest_type': None,
                         'cds_digest': None, 'cdnskey_flags': None, 'cdnskey_protocol': None, 'cdnskey_algorithm': None,
                         'cdnskey_pk_rsa_n': None, 'cdnskey_pk_rsa_e': None, 'cdnskey_pk_rsa_bitsize': None,
                         'cdnskey_pk_eccgost_x': None, 'cdnskey_pk_eccgost_y': None, 'cdnskey_pk_dsa_t': None,
                         'cdnskey_pk_dsa_q': None, 'cdnskey_pk_dsa_p': None, 'cdnskey_pk_dsa_g': None,
                         'cdnskey_pk_dsa_y': None, 'cdnskey_pk_eddsa_a': None, 'cdnskey_pk_wire': None,
                         'caa_flags': None,
                         'caa_tag': None, 'caa_value': None, 'tlsa_usage': None, 'tlsa_selector': None,
                         'tlsa_matchtype': None, 'tlsa_certdata': None, 'ptr_name': None}

    umbrella_a_struct = {'query_type': 'A', 'query_name': '1.rpn-news3.club.', 'response_type': 'A',
                         'response_name': 'rpland.gdns.revopush.com.', 'response_ttl': 30, 'timestamp': 1577841412000,
                         'rtt': 0.04292553663253784, 'worker_id': 423410526, 'status_code': 0,
                         'ip4_address': '148.251.178.252', 'ip6_address': None, 'country': 'DE', 'as': '24940',
                         'as_full': '[24940]', 'ip_prefix': '148.251.0.0/16', 'cname_name': None, 'dname_name': None,
                         'mx_address': None, 'mx_preference': None, 'mxset_hash_algorithm': None, 'mxset_hash': None,
                         'ns_address': None, 'nsset_hash_algorithm': None, 'nsset_hash': None, 'txt_text': None,
                         'txt_hash_algorithm': None, 'txt_hash': None, 'ds_key_tag': None, 'ds_algorithm': None,
                         'ds_digest_type': None, 'ds_digest': None, 'dnskey_flags': None, 'dnskey_protocol': None,
                         'dnskey_algorithm': None, 'dnskey_pk_rsa_n': None, 'dnskey_pk_rsa_e': None,
                         'dnskey_pk_rsa_bitsize': None, 'dnskey_pk_eccgost_x': None, 'dnskey_pk_eccgost_y': None,
                         'dnskey_pk_dsa_t': None, 'dnskey_pk_dsa_q': None, 'dnskey_pk_dsa_p': None,
                         'dnskey_pk_dsa_g': None, 'dnskey_pk_dsa_y': None, 'dnskey_pk_eddsa_a': None,
                         'dnskey_pk_wire': None, 'nsec_next_domain_name': None, 'nsec_owner_rrset_types': None,
                         'nsec3_hash_algorithm': None, 'nsec3_flags': None, 'nsec3_iterations': None,
                         'nsec3_salt': None, 'nsec3_next_domain_name_hash': None, 'nsec3_owner_rrset_types': None,
                         'nsec3param_hash_algorithm': None, 'nsec3param_flags': None, 'nsec3param_iterations': None,
                         'nsec3param_salt': None, 'spf_text': None, 'spf_hash_algorithm': None, 'spf_hash': None,
                         'soa_mname': None, 'soa_rname': None, 'soa_serial': None, 'soa_refresh': None,
                         'soa_retry': None, 'soa_expire': None, 'soa_minimum': None, 'rrsig_type_covered': None,
                         'rrsig_algorithm': None, 'rrsig_labels': None, 'rrsig_original_ttl': None,
                         'rrsig_signature_inception': None, 'rrsig_signature_expiration': None, 'rrsig_key_tag': None,
                         'rrsig_signer_name': None, 'rrsig_signature': None, 'cds_key_tag': None, 'cds_algorithm': None,
                         'cds_digest_type': None, 'cds_digest': None, 'cdnskey_flags': None, 'cdnskey_protocol': None,
                         'cdnskey_algorithm': None, 'cdnskey_pk_rsa_n': None, 'cdnskey_pk_rsa_e': None,
                         'cdnskey_pk_rsa_bitsize': None, 'cdnskey_pk_eccgost_x': None, 'cdnskey_pk_eccgost_y': None,
                         'cdnskey_pk_dsa_t': None, 'cdnskey_pk_dsa_q': None, 'cdnskey_pk_dsa_p': None,
                         'cdnskey_pk_dsa_g': None, 'cdnskey_pk_dsa_y': None, 'cdnskey_pk_eddsa_a': None,
                         'cdnskey_pk_wire': None, 'caa_flags': None, 'caa_tag': None, 'caa_value': None,
                         'tlsa_usage': None, 'tlsa_selector': None, 'tlsa_matchtype': None, 'tlsa_certdata': None,
                         'ptr_name': None}
    umbrella_cname_struct = {'query_type': 'SOA', 'query_name': '1.rpn-news3.club.', 'response_type': 'CNAME',
                             'response_name': '1.rpn-news3.club.', 'response_ttl': 1800, 'timestamp': 1577841412000,
                             'rtt': 0.05287587642669678, 'worker_id': 423410526, 'status_code': 0, 'ip4_address': None,
                             'ip6_address': None, 'country': None, 'as': None, 'as_full': None, 'ip_prefix': None,
                             'cname_name': 'rpland.gdns.revopush.com.', 'dname_name': None, 'mx_address': None,
                             'mx_preference': None, 'mxset_hash_algorithm': None, 'mxset_hash': None,
                             'ns_address': None, 'nsset_hash_algorithm': None, 'nsset_hash': None, 'txt_text': None,
                             'txt_hash_algorithm': None, 'txt_hash': None, 'ds_key_tag': None, 'ds_algorithm': None,
                             'ds_digest_type': None, 'ds_digest': None, 'dnskey_flags': None, 'dnskey_protocol': None,
                             'dnskey_algorithm': None, 'dnskey_pk_rsa_n': None, 'dnskey_pk_rsa_e': None,
                             'dnskey_pk_rsa_bitsize': None, 'dnskey_pk_eccgost_x': None, 'dnskey_pk_eccgost_y': None,
                             'dnskey_pk_dsa_t': None, 'dnskey_pk_dsa_q': None, 'dnskey_pk_dsa_p': None,
                             'dnskey_pk_dsa_g': None, 'dnskey_pk_dsa_y': None, 'dnskey_pk_eddsa_a': None,
                             'dnskey_pk_wire': None, 'nsec_next_domain_name': None, 'nsec_owner_rrset_types': None,
                             'nsec3_hash_algorithm': None, 'nsec3_flags': None, 'nsec3_iterations': None,
                             'nsec3_salt': None, 'nsec3_next_domain_name_hash': None, 'nsec3_owner_rrset_types': None,
                             'nsec3param_hash_algorithm': None, 'nsec3param_flags': None, 'nsec3param_iterations': None,
                             'nsec3param_salt': None, 'spf_text': None, 'spf_hash_algorithm': None, 'spf_hash': None,
                             'soa_mname': None, 'soa_rname': None, 'soa_serial': None, 'soa_refresh': None,
                             'soa_retry': None, 'soa_expire': None, 'soa_minimum': None, 'rrsig_type_covered': None,
                             'rrsig_algorithm': None, 'rrsig_labels': None, 'rrsig_original_ttl': None,
                             'rrsig_signature_inception': None, 'rrsig_signature_expiration': None,
                             'rrsig_key_tag': None, 'rrsig_signer_name': None, 'rrsig_signature': None,
                             'cds_key_tag': None, 'cds_algorithm': None, 'cds_digest_type': None, 'cds_digest': None,
                             'cdnskey_flags': None, 'cdnskey_protocol': None, 'cdnskey_algorithm': None,
                             'cdnskey_pk_rsa_n': None, 'cdnskey_pk_rsa_e': None, 'cdnskey_pk_rsa_bitsize': None,
                             'cdnskey_pk_eccgost_x': None, 'cdnskey_pk_eccgost_y': None, 'cdnskey_pk_dsa_t': None,
                             'cdnskey_pk_dsa_q': None, 'cdnskey_pk_dsa_p': None, 'cdnskey_pk_dsa_g': None,
                             'cdnskey_pk_dsa_y': None, 'cdnskey_pk_eddsa_a': None, 'cdnskey_pk_wire': None,
                             'caa_flags': None, 'caa_tag': None, 'caa_value': None, 'tlsa_usage': None,
                             'tlsa_selector': None, 'tlsa_matchtype': None, 'tlsa_certdata': None, 'ptr_name': None}
    umbrella_aaaa_struct = {'query_type': 'AAAA', 'query_name': 'basvformu7.xyz.', 'response_type': 'AAAA',
                            'response_name': 'basvformu7.xyz.', 'response_ttl': 300, 'timestamp': 1577841412000,
                            'rtt': 0.008681416511535645, 'worker_id': 423410526, 'status_code': 0, 'ip4_address': None,
                            'ip6_address': '2606:4700:30::681c:8db', 'country': 'US', 'as': '13335',
                            'as_full': '[13335]', 'ip_prefix': '2606:4700:30::/44', 'cname_name': None,
                            'dname_name': None, 'mx_address': None, 'mx_preference': None, 'mxset_hash_algorithm': None,
                            'mxset_hash': None, 'ns_address': None, 'nsset_hash_algorithm': None, 'nsset_hash': None,
                            'txt_text': None, 'txt_hash_algorithm': None, 'txt_hash': None, 'ds_key_tag': None,
                            'ds_algorithm': None, 'ds_digest_type': None, 'ds_digest': None, 'dnskey_flags': None,
                            'dnskey_protocol': None, 'dnskey_algorithm': None, 'dnskey_pk_rsa_n': None,
                            'dnskey_pk_rsa_e': None, 'dnskey_pk_rsa_bitsize': None, 'dnskey_pk_eccgost_x': None,
                            'dnskey_pk_eccgost_y': None, 'dnskey_pk_dsa_t': None, 'dnskey_pk_dsa_q': None,
                            'dnskey_pk_dsa_p': None, 'dnskey_pk_dsa_g': None, 'dnskey_pk_dsa_y': None,
                            'dnskey_pk_eddsa_a': None, 'dnskey_pk_wire': None, 'nsec_next_domain_name': None,
                            'nsec_owner_rrset_types': None, 'nsec3_hash_algorithm': None, 'nsec3_flags': None,
                            'nsec3_iterations': None, 'nsec3_salt': None, 'nsec3_next_domain_name_hash': None,
                            'nsec3_owner_rrset_types': None, 'nsec3param_hash_algorithm': None,
                            'nsec3param_flags': None, 'nsec3param_iterations': None, 'nsec3param_salt': None,
                            'spf_text': None, 'spf_hash_algorithm': None, 'spf_hash': None, 'soa_mname': None,
                            'soa_rname': None, 'soa_serial': None, 'soa_refresh': None, 'soa_retry': None,
                            'soa_expire': None, 'soa_minimum': None, 'rrsig_type_covered': None,
                            'rrsig_algorithm': None, 'rrsig_labels': None, 'rrsig_original_ttl': None,
                            'rrsig_signature_inception': None, 'rrsig_signature_expiration': None,
                            'rrsig_key_tag': None, 'rrsig_signer_name': None, 'rrsig_signature': None,
                            'cds_key_tag': None, 'cds_algorithm': None, 'cds_digest_type': None, 'cds_digest': None,
                            'cdnskey_flags': None, 'cdnskey_protocol': None, 'cdnskey_algorithm': None,
                            'cdnskey_pk_rsa_n': None, 'cdnskey_pk_rsa_e': None, 'cdnskey_pk_rsa_bitsize': None,
                            'cdnskey_pk_eccgost_x': None, 'cdnskey_pk_eccgost_y': None, 'cdnskey_pk_dsa_t': None,
                            'cdnskey_pk_dsa_q': None, 'cdnskey_pk_dsa_p': None, 'cdnskey_pk_dsa_g': None,
                            'cdnskey_pk_dsa_y': None, 'cdnskey_pk_eddsa_a': None, 'cdnskey_pk_wire': None,
                            'caa_flags': None, 'caa_tag': None, 'caa_value': None, 'tlsa_usage': None,
                            'tlsa_selector': None, 'tlsa_matchtype': None, 'tlsa_certdata': None, 'ptr_name': None}

    def test_the_alexa_struct(self):
        a_struct = self.alexa_a_struct
        cname_struct = self.alexa_cname_struct
        aaaa_struct = self.alexa_aaaa_struct
        create_sql_a = infer_create_table_sql("A_table", a_struct)
        create_sql_cname = infer_create_table_sql("A_table", cname_struct)
        create_sql_aaaa = infer_create_table_sql("A_table", aaaa_struct)
        self.assertTrue(create_sql_a == create_sql_cname)
        self.assertTrue(create_sql_aaaa == create_sql_cname)

    def test_the_umbrella_struct(self):
        a_struct = self.umbrella_a_struct
        cname_struct = self.umbrella_cname_struct
        aaaa_struct = self.umbrella_aaaa_struct
        create_sql_a = infer_create_table_sql("A_table", a_struct)
        print(create_sql_a)
        create_sql_cname = infer_create_table_sql("A_table", cname_struct)
        create_sql_aaaa = infer_create_table_sql("A_table", aaaa_struct)
        self.assertTrue(create_sql_a == create_sql_cname)
        self.assertTrue(create_sql_aaaa == create_sql_cname)


class TestDao(TestCase):
    db = './data/example.db'

    def test_multiple_create(self):
        dao = Dao.load_table(self.db, "Alexa")
        dao.insert_data(Test.alexa_cname_struct)
        dao.close()

    def test_read_data(self):
        dao = Dao.load_table(self.db, "Alexa")
        rd = dao.read_data("SELECT * FROM Alexa WHERE response_type == 'CNAME'")
        print(rd)
        dao.close()
