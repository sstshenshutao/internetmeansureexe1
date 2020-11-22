from question_d import Asn

# default 50 processes
asn = Asn("alexa20201015.db", 'Alexa', year=2020, month=10, chunksize=1000, process_number=50)
asn.flush_ases()
