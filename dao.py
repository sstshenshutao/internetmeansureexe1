import os
import sqlite3
import pandas as pd

# alexa_table_tuple: the sql struct
alexa_table_tuple = (
    "query_type", "query_name", "response_type", "response_name", "response_ttl", "timestamp", "rtt", "worker_id",
    "status_code", "ad_flag", "ip4_address", "ip6_address", "country", "as", "as_full", "ip_prefix", "cname_name",
    "dname_name", "mx_address", "mx_preference", "mxset_hash_algorithm", "mxset_hash", "ns_address",
    "nsset_hash_algorithm",
    "nsset_hash", "txt_text", "txt_hash_algorithm", "txt_hash", "ds_key_tag", "ds_algorithm", "ds_digest_type",
    "ds_digest",
    "dnskey_flags", "dnskey_protocol", "dnskey_algorithm", "dnskey_pk_rsa_n", "dnskey_pk_rsa_e",
    "dnskey_pk_rsa_bitsize",
    "dnskey_pk_eccgost_x", "dnskey_pk_eccgost_y", "dnskey_pk_dsa_t", "dnskey_pk_dsa_q", "dnskey_pk_dsa_p",
    "dnskey_pk_dsa_g", "dnskey_pk_dsa_y", "dnskey_pk_eddsa_a", "dnskey_pk_wire", "nsec_next_domain_name",
    "nsec_owner_rrset_types", "nsec3_hash_algorithm", "nsec3_flags", "nsec3_iterations", "nsec3_salt",
    "nsec3_next_domain_name_hash", "nsec3_owner_rrset_types", "nsec3param_hash_algorithm", "nsec3param_flags",
    "nsec3param_iterations", "nsec3param_salt", "spf_text", "spf_hash_algorithm", "spf_hash", "soa_mname", "soa_rname",
    "soa_serial", "soa_refresh", "soa_retry", "soa_expire", "soa_minimum", "rrsig_type_covered", "rrsig_algorithm",
    "rrsig_labels", "rrsig_original_ttl", "rrsig_signature_inception", "rrsig_signature_expiration", "rrsig_key_tag",
    "rrsig_signer_name", "rrsig_signature", "cds_key_tag", "cds_algorithm", "cds_digest_type", "cds_digest",
    "cdnskey_flags", "cdnskey_protocol", "cdnskey_algorithm", "cdnskey_pk_rsa_n", "cdnskey_pk_rsa_e",
    "cdnskey_pk_rsa_bitsize", "cdnskey_pk_eccgost_x", "cdnskey_pk_eccgost_y", "cdnskey_pk_dsa_t", "cdnskey_pk_dsa_q",
    "cdnskey_pk_dsa_p", "cdnskey_pk_dsa_g", "cdnskey_pk_dsa_y", "cdnskey_pk_eddsa_a", "cdnskey_pk_wire", "caa_flags",
    "caa_tag", "caa_value", "tlsa_usage", "tlsa_selector", "tlsa_matchtype", "tlsa_certdata", "ptr_name")
umbrella_table_tuple = (
    "query_type", "query_name", "response_type", "response_name", "response_ttl", "timestamp", "rtt", "worker_id",
    "status_code", "ip4_address", "ip6_address", "country", "as", "as_full", "ip_prefix", "cname_name", "dname_name",
    "mx_address", "mx_preference", "mxset_hash_algorithm", "mxset_hash", "ns_address", "nsset_hash_algorithm",
    "nsset_hash", "txt_text", "txt_hash_algorithm", "txt_hash", "ds_key_tag", "ds_algorithm", "ds_digest_type",
    "ds_digest", "dnskey_flags", "dnskey_protocol", "dnskey_algorithm", "dnskey_pk_rsa_n", "dnskey_pk_rsa_e",
    "dnskey_pk_rsa_bitsize", "dnskey_pk_eccgost_x", "dnskey_pk_eccgost_y", "dnskey_pk_dsa_t", "dnskey_pk_dsa_q",
    "dnskey_pk_dsa_p", "dnskey_pk_dsa_g", "dnskey_pk_dsa_y", "dnskey_pk_eddsa_a", "dnskey_pk_wire",
    "nsec_next_domain_name", "nsec_owner_rrset_types", "nsec3_hash_algorithm", "nsec3_flags", "nsec3_iterations",
    "nsec3_salt", "nsec3_next_domain_name_hash", "nsec3_owner_rrset_types", "nsec3param_hash_algorithm",
    "nsec3param_flags", "nsec3param_iterations", "nsec3param_salt", "spf_text", "spf_hash_algorithm", "spf_hash",
    "soa_mname", "soa_rname", "soa_serial", "soa_refresh", "soa_retry", "soa_expire", "soa_minimum",
    "rrsig_type_covered",
    "rrsig_algorithm", "rrsig_labels", "rrsig_original_ttl", "rrsig_signature_inception", "rrsig_signature_expiration",
    "rrsig_key_tag", "rrsig_signer_name", "rrsig_signature", "cds_key_tag", "cds_algorithm", "cds_digest_type",
    "cds_digest", "cdnskey_flags", "cdnskey_protocol", "cdnskey_algorithm", "cdnskey_pk_rsa_n", "cdnskey_pk_rsa_e",
    "cdnskey_pk_rsa_bitsize", "cdnskey_pk_eccgost_x", "cdnskey_pk_eccgost_y", "cdnskey_pk_dsa_t", "cdnskey_pk_dsa_q",
    "cdnskey_pk_dsa_p", "cdnskey_pk_dsa_g", "cdnskey_pk_dsa_y", "cdnskey_pk_eddsa_a", "cdnskey_pk_wire", "caa_flags",
    "caa_tag", "caa_value", "tlsa_usage", "tlsa_selector", "tlsa_matchtype", "tlsa_certdata", "ptr_name")


class Dao:
    """
    Dao(Data Access Object): is a class for saving the records of the avro to the sqlite3
    """

    def __init__(self, db_name, table_name, table_tuple, buffer_max):
        self.table_name = table_name
        self.table_tuple = table_tuple
        conn = sqlite3.connect(db_name, isolation_level='EXCLUSIVE')
        self.conn = conn
        cursor = conn.cursor()
        self._c = cursor
        self._create_table()
        # buffered size of insertion
        self.buffer_max = buffer_max
        self._buffer = []

    def _create_table(self):
        sql_command = self._generate_create_table_sql()
        self._c.execute(sql_command)

    @classmethod
    def load_table(cls, db_name, table_name, buffer_max=0):
        if table_name == 'Alexa':
            return cls(db_name, table_name, alexa_table_tuple, buffer_max)
        elif table_name == 'Umbrella':
            return cls(db_name, table_name, umbrella_table_tuple, buffer_max)
        else:
            raise Exception("only support Alexa and Umbrella")

    def _generate_create_table_sql(self):
        sql_command = "CREATE TABLE IF NOT EXISTS " + self.table_name + "\n("
        is_first = True
        for k in self.table_tuple:
            if is_first:
                # escape "as" => "as_key"
                sql_command += self._escape_key(k)
                is_first = False
            else:
                sql_command += (", " + self._escape_key(k))
        sql_command += ")"
        return sql_command

    def _escape_key(self, key_str):
        return "'" + key_str + "'"

    def flush(self, lock):
        sql_command = self._generate_insert_table_sql()
        lock.acquire()
        self.conn.executemany(sql_command, self._buffer)
        try:
            self.conn.commit()
        except Exception as e:
            print("except!!!" + str(type(e)))
        else:
            self._buffer = []
        finally:
            lock.release()

    def insert_all_records(self, records, lock):
        self._buffer = [self.reform(r) for r in records]
        self.flush(lock)

    def insert_data(self, data, lock):
        self._buffer.append(self.reform(data))
        if len(self._buffer) >= self.buffer_max:
            self.flush(lock)
        # self._c.execute(sql_command)
        # save the changes
        # self._conn.commit()

    def create_column(self, column_name, data_type):
        sql_command = "alter table %s add '%s' %s;" % (self.table_name, column_name, data_type)
        self._c.execute(sql_command)
        self.conn.commit()

    def close(self):
        self.conn.close()

    def _generate_insert_table_sql(self):
        sql_command = "INSERT INTO " + self.table_name + " VALUES ("
        is_first = True
        for _ in self.table_tuple:
            if is_first:
                sql_command += '?'
                is_first = False
            else:
                sql_command += ", ?"
        sql_command += ')'
        # print(sql_command)
        return sql_command

    def reform(self, data):
        tmp = []
        for k in self.table_tuple:
            tmp.append(data[k])
        return tuple(tmp)

    # def _generate_insert_table_sql(self, data):
    #     sql_command = "INSERT INTO " + self.table_name + " VALUES ("
    #     is_first = True
    #     for k in self.table_tuple:
    #         if is_first:
    #             sql_command += self._escape_value(data[k])
    #             is_first = False
    #         else:
    #             sql_command += (", " + self._escape_value(data[k]))
    #     sql_command += ')'
    #     # print(sql_command)
    #     return sql_command

    def _escape_value(self, value):
        t = type(value)
        if t == int or t == float:
            return str(value)
        else:
            return "'" + str(value) + "'"

    def read_data(self, sql_command):
        return pd.read_sql(sql_command, self.conn)
